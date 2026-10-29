"""Synthetic evidence stays in TemporaryDirectory; no benchmark results are manufactured."""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from phase3_config import load_phase3, cost_plan
from calculate_parallelism import calculate, calculate_operators, neighbors
from capacity_analysis import bounds, next_rate, search_verdict, estimate_operator_capacity, diagnose, comparable_fingerprint
from experiment_engine import execute, prediction, evaluate_validation
from analyze_results import flatten, gain_analysis, repeat_statistics
from run_once import job_args
from preflight import session_args


def attempt(rate, verdict, ids=None):
    return {'rate': rate, 'verdict': verdict, 'run_ids': ids or []}


def diagnostic(p, op='heavy', capacity=100, fingerprint='same'):
    return {'mode': 'full', 'execution_validity': 'VALID', 'bottleneck_candidate': op,
            'resource_saturation': [], 'resource_evidence_missing': False, 'fingerprint': fingerprint,
            'operators': {op: {'observed_rate_per_subtask_mean': capacity}},
            'resources': {'taskmanager': {'cpu_quota_utilization': {'max': .5}, 'throttled_period_ratio': {'max': 0}}}}


class FormulaTest(unittest.TestCase):
    def test_required_parallelism_and_headroom(self):
        r = calculate(40000, 7000, .7)
        self.assertEqual(r['required_parallelism'], 9)
        self.assertAlmostEqual(r['estimated_load_per_subtask'], 40000 / 9)
        self.assertEqual(r['estimated_headroom_records_per_second'], 23000)

    def test_exact_boundary_and_invalid_input(self):
        self.assertEqual(calculate(490, 700, .7)['required_parallelism'], 1)
        for value in [0, -1, float('nan'), float('inf'), True]:
            with self.assertRaises(ValueError): calculate(100, value, .7)
        with self.assertRaises(ValueError): calculate(100, 100, 1.1)

    def test_operator_inputs_and_neighbors(self):
        rows = calculate_operators({'a': {'peak_input_rate': 100, 'subtask_capacity': 100, 'target_utilization': .5},
                                    'b': {'peak_input_rate': 50, 'subtask_capacity': 100, 'target_utilization': .5}})
        self.assertEqual(rows['a']['required_parallelism'], 2); self.assertEqual(rows['b']['required_parallelism'], 1)
        self.assertEqual(neighbors(9), [7, 8, 9, 10, 12]); self.assertEqual(neighbors(1), [1, 2, 4])


class DiscoveryTest(unittest.TestCase):
    def setUp(self): self.plan = load_phase3('full')['plan']

    def test_increment_refine_resolution_and_attempt_limit(self):
        self.assertEqual(next_rate([], self.plan), 1000)
        rows = [attempt(1000, 'STABLE')]; self.assertEqual(next_rate(rows, self.plan), 2000)
        rows += [attempt(2000, 'UNSTABLE')]; self.assertEqual(next_rate(rows, self.plan), 1500)
        rows += [attempt(1500, 'STABLE'), attempt(1600, 'UNSTABLE')]; self.assertIsNone(next_rate(rows, self.plan))
        self.assertIsNone(next_rate(rows, self.plan | {'max_attempts': 2}))

    def test_initial_overload_searches_lower_rate(self):
        self.assertEqual(next_rate([attempt(1000, 'UNSTABLE')], self.plan), 500)
        self.assertIsNone(next_rate([attempt(10, 'UNSTABLE')], self.plan))

    def test_unknown_invalid_and_checkpoint_failures_not_capacity_bounds(self):
        good = {'execution_validity': 'VALID', 'performance_stability': 'UNSTABLE', 'reasons': ['measured backlog growth']}
        self.assertEqual(search_verdict([good] * 3), 'UNSTABLE')
        for changed in [good | {'execution_validity': 'INVALID'}, good | {'performance_stability': 'UNKNOWN'},
                        good | {'reasons': ['latency growth or checkpoint failures']}]:
            self.assertEqual(search_verdict([good, changed]), 'INCONCLUSIVE')
        rows = [attempt(1000, 'INCONCLUSIVE')]; self.assertIsNone(next_rate(rows, self.plan))
        self.assertIsNone(bounds(rows, .1)['stable_input_lower_bound'])

    def test_right_censor_and_contradiction_preserved(self):
        r = bounds([attempt(32000, 'STABLE')], .1)
        self.assertEqual(r['status'], 'RIGHT_CENSORED'); self.assertIsNone(r['maximum_capacity'])
        r = bounds([attempt(100, 'STABLE'), attempt(90, 'UNSTABLE')], .1)
        self.assertEqual(r['status'], 'CONTRADICTORY'); self.assertIsNone(r['resolution_records_per_second'])

    def test_stable_target_and_observed_processing_rate_separate(self):
        a = attempt(100, 'STABLE') | {'observed_sustainable_throughputs': [98, 99, 97]}
        r = bounds([a], .1)
        self.assertEqual(r['stable_input_lower_bound'], 100)
        self.assertEqual(r['observed_sustainable_throughput_lower_bound'], 97)


class CapacityEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.thresholds = load_phase3()['evidence_thresholds']
        self.searches = {}; self.diags = {}
        for p in [2, 4]:
            a, b = f'stable{p}', f'upper{p}'
            self.searches[str(p)] = bounds([attempt(100 * p, 'STABLE', [a]), attempt(110 * p, 'UNSTABLE', [b])], .1)
            self.diags[a] = diagnostic(p); self.diags[b] = diagnostic(p)

    def test_direct_operator_estimate_multiple_parallelisms(self):
        r = estimate_operator_capacity(self.searches, self.diags, self.thresholds)
        self.assertEqual(r['status'], 'CONDITIONAL_ESTIMATE'); self.assertEqual(r['operator'], 'heavy')
        self.assertEqual(r['subtask_capacity'], 100)
        self.assertEqual(len(r['source_run_ids']), 4)

    def test_smoke_source_bottleneck_contention_disagreement_missing_rejected(self):
        for key, value in [('mode', 'smoke'), ('bottleneck_candidate', 'source'), ('resource_saturation', ['TM saturated']),
                           ('resource_evidence_missing', True), ('fingerprint', 'changed'), ('bottleneck_candidate', None)]:
            with self.subTest(key=key, value=value):
                ds = copy.deepcopy(self.diags); ds['stable2'][key] = value
                self.assertEqual(estimate_operator_capacity(self.searches, ds, self.thresholds)['status'], 'NOT_ESTIMATED')
        ds = copy.deepcopy(self.diags); ds['stable4']['operators']['heavy']['observed_rate_per_subtask_mean'] = 400
        self.assertEqual(estimate_operator_capacity(self.searches, ds, self.thresholds)['status'], 'NOT_ESTIMATED')

    def test_cpu_budget_fingerprint_excludes_rate_and_seed_but_not_workload(self):
        cfg = load_phase3()['run_config']; m = {'config': cfg, 'environment': {'host': {'cpu_affinity_count': 4, 'load_average': [0, 0, 0]}}}
        changed = copy.deepcopy(m); changed['config'].update(seed=99, target_rate=999)
        changed['environment']['host']['load_average'] = [1, 2, 3]
        self.assertEqual(comparable_fingerprint(m), comparable_fingerprint(changed))
        changed['config']['cpu_work_iterations'] += 1
        self.assertNotEqual(comparable_fingerprint(m), comparable_fingerprint(changed))

    def test_counter_reset_and_sink_completion_used_in_diagnosis(self):
        cfg = load_phase3()['run_config']; cfg['operator_parallelism'] = {op: 1 for op in cfg['operator_parallelism']}
        names = {'source': 'Source: Kafka Source', 'parse': 'Parse', 'heavy': 'Heavy CPU', 'aggregation': 'Stateful Aggregation', 'sink': 'Measurement Sink: Writer'}
        with tempfile.TemporaryDirectory() as td:
            path = Path(td)
            (path / 'metadata.json').write_text(json.dumps({'config': cfg, 'mode': 'smoke', 'plan': {'measurement_start_wall_ms': 0, 'measurement_end_wall_ms': 20000}}))
            (path / 'summary.json').write_text(json.dumps({'run_id': 'fixture', 'execution_validity': 'VALID', 'performance_stability': 'UNKNOWN', 'observations': {}}))
            samples = []
            for t, count in [(1000, 10), (6000, 100), (11000, 1)]:
                samples.append({'started_wall_ms': t, 'wall_time_ms': t, 'resources': {}, 'subtasks': [
                    {'operator': names[op], 'subtask': 0, 'metrics': {'numRecordsOut': count if op == 'heavy' else 0,
                     'completedRecords': t // 1000, 'busyTimeMsPerSecond': 100, 'idleTimeMsPerSecond': 900, 'backPressuredTimeMsPerSecond': 0}}
                    for op in names]})
            (path / 'raw_samples.jsonl').write_text(''.join(json.dumps(s) + '\n' for s in samples))
            d = diagnose(path, self.thresholds)
            self.assertIsNone(d['operators']['heavy']['observed_output_rate'])
            self.assertEqual(d['operators']['sink']['observed_output_rate'], 1)
            self.assertIsNone(d['operators']['sink']['capacity_per_subtask'])


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self.cfg = load_phase3(); self.capacity = {'status': 'CONDITIONAL_ESTIMATE', 'operator': 'heavy', 'subtask_capacity': 100,
            'source_run_ids': ['calibration'], 'entries': [{'fingerprint': 'same'}]}
        self.pred = prediction(self.capacity, self.cfg['plan'] | {'peak_input_rate': 200})

    def row(self, p, rid, validity='VALID', stability='STABLE'):
        return {'parallelism': p, 'run_id': rid, 'execution_validity': validity, 'performance_stability': stability,
                'observations': {'latency': {'p99': 100}, 'checkpoint_latest': {'completed': {'end_to_end_duration': 100}}}}

    def test_minimum_tested_not_global_and_operational_headroom(self):
        rows = [self.row(2, 'v2'), self.row(3, 'v3')]; ds = {r['run_id']: diagnostic(r['parallelism']) for r in rows}
        r = evaluate_validation(rows, ds, self.pred, self.cfg['operational_policy'], 1)
        self.assertEqual(r['predicted_required_parallelism'], 3)
        self.assertEqual(r['measured_minimum_stable_parallelism'], 2)
        self.assertEqual(r['recommended_operational_parallelism'], 3)
        self.assertEqual(r['calculation_error_parallelism'], 1)
        self.assertEqual([c['target_utilization'] for c in self.pred['sensitivity']], [.6, .7, .8])

    def test_overlap_incomplete_repeats_resource_change_and_smoke(self):
        with self.assertRaises(ValueError): evaluate_validation([self.row(2, 'calibration')], {}, self.pred, self.cfg['operational_policy'], 1)
        rows = [self.row(2, 'v')]; ds = {'v': diagnostic(2)}
        self.assertIsNone(evaluate_validation(rows, ds, self.pred, self.cfg['operational_policy'], 3)['measured_minimum_stable_parallelism'])
        ds['v']['fingerprint'] = 'changed'
        self.assertIsNone(evaluate_validation(rows, ds, self.pred, self.cfg['operational_policy'], 1)['measured_minimum_stable_parallelism'])
        rows[0]['performance_stability'] = 'UNKNOWN'
        self.assertIsNone(evaluate_validation(rows, ds, self.pred, self.cfg['operational_policy'], 1)['measured_minimum_stable_parallelism'])


class SessionTest(unittest.TestCase):
    def test_smoke_orchestration_separates_roles_and_preserves_failures(self):
        cfg = load_phase3(overrides={'parallelisms': [1], 'calibration_parallelisms': [1]})
        with tempfile.TemporaryDirectory() as td:
            counter = 0
            def runner(run_cfg, mode, base, output_dir, experiment_id, context):
                nonlocal counter
                counter += 1; path = output_dir / str(counter); path.mkdir(parents=True)
                return {'run_id': str(counter), 'result_directory': str(path), 'execution_validity': 'VALID',
                        'performance_stability': 'UNKNOWN', 'observations': {}, 'cleanup_errors': []}
            def d(path, thresholds): return diagnostic(1) | {'mode': 'smoke'}
            with patch('builtins.print'):
                path, result = execute(cfg, 2, Path(td), runner=runner, diagnostician=d)
            self.assertEqual(result['run_count'], 3)
            self.assertIsNone(result['prediction']['required_parallelism'])
            manifest = [json.loads(l) for l in (path / 'runs.jsonl').read_text().splitlines()]
            self.assertEqual(manifest[0]['role'], 'calibration')
            self.assertTrue(all(r['role'] == 'uncalibrated_peak_probe' for r in manifest[1:]))
            self.assertFalse(set(result['validation']['validation_run_ids']) & set(result['prediction']['capacity']['source_run_ids']))
            self.assertTrue((path / 'summary.csv').exists())

    def test_phase2_defaults_and_actual_experiment_id_plumbed(self):
        cfg = load_phase3()['run_config']
        args = job_args(cfg, 'topic', 'group', 'run', 10, 'experiment_1')
        self.assertEqual(args[args.index('--experiment-id') + 1], 'experiment_1')
        args = session_args(cfg, ROOT / 'results/fixture', 't', 'g', 'r', 0, 10, experiment_id='experiment_2')
        self.assertEqual(args[args.index('--experiment-id') + 1], 'experiment_2')

    def test_full_prediction_is_frozen_before_neighbor_runs_and_ids_disjoint(self):
        cfg = load_phase3('full', overrides={'calibration_parallelisms': [2, 4], 'initial_rate': 100,
                                         'max_rate': 1000, 'peak_input_rate': 200, 'random_order': False})
        with tempfile.TemporaryDirectory() as td:
            counter = 0
            def runner(run_cfg, mode, base, output_dir, experiment_id, context):
                nonlocal counter
                counter += 1; path = output_dir / str(counter); path.mkdir(parents=True)
                (path / 'fixture_config.json').write_text(json.dumps(run_cfg))
                if context['role'] == 'validation':
                    frozen = json.loads((output_dir.parent / 'prediction.json').read_text())
                    self.assertEqual(frozen['required_parallelism'], 3)
                    self.assertTrue(frozen['calibration_run_ids'])
                stable = run_cfg['target_rate'] <= run_cfg['operator_parallelism']['heavy'] * 100
                return {'run_id': str(counter), 'result_directory': str(path), 'execution_validity': 'VALID',
                        'performance_stability': 'STABLE' if stable else 'UNSTABLE',
                        'reasons': [] if stable else ['measured backlog growth'],
                        'observations': {'latency': {'p99': 100}, 'checkpoint_latest': {'completed': {'end_to_end_duration': 100}}}, 'cleanup_errors': []}
            def d(path, thresholds):
                c = json.loads((path / 'fixture_config.json').read_text()); p = c['operator_parallelism']['heavy']
                return diagnostic(p, capacity=min(100, c['target_rate'] / p))
            with patch('builtins.print'):
                path, result = execute(cfg, 2, Path(td), runner=runner, diagnostician=d)
            self.assertEqual(result['status'], 'COMPLETED')
            self.assertEqual(result['prediction']['required_parallelism'], 3)
            self.assertEqual(result['validation']['measured_minimum_stable_parallelism'], 2)
            self.assertFalse(set(result['validation']['calibration_run_ids']) & set(result['validation']['validation_run_ids']))

    def test_full_repetitions_config_and_cost(self):
        with self.assertRaises(ValueError): load_phase3('full', overrides={'repetitions': 1})
        with self.assertRaises(ValueError): load_phase3(overrides={'max_attempts': 0})
        with self.assertRaises(ValueError): load_phase3(overrides={'parallelisms': [1, 1]})
        self.assertEqual(cost_plan(load_phase3(), 1)['maximum_run_count'], 4)

    def test_failed_cleanup_preserves_run_and_stops_next_case(self):
        cfg = load_phase3()
        with tempfile.TemporaryDirectory() as td:
            calls = []
            def runner(run_cfg, mode, base, output_dir, experiment_id, context):
                calls.append(context); path = output_dir / 'fixture'; path.mkdir(parents=True)
                return {'run_id': 'fixture', 'result_directory': str(path), 'execution_validity': 'ERROR',
                        'performance_stability': 'UNKNOWN', 'observations': {}, 'cleanup_errors': ['fixture cancellation failure']}
            with patch('builtins.print'):
                path, result = execute(cfg, 1, Path(td), runner=runner, diagnostician=lambda *_: diagnostic(1))
            self.assertEqual(len(calls), 1); self.assertEqual(result['status'], 'ERROR')
            self.assertTrue((path / 'runs.jsonl').exists()); self.assertTrue((path / 'session_summary.json').exists())


class SummaryTest(unittest.TestCase):
    def test_no_knee_from_censored_equal_search_cap(self):
        searches = {str(p): bounds([attempt(100, 'STABLE')], .1) for p in [1, 2]}
        self.assertEqual(gain_analysis(searches, .15)['knee_candidates'], [])

    def test_missing_metrics_not_zero_in_flat_summary(self):
        r = {'run_id': 'r', 'role': 'fixed_input', 'parallelism': 1, 'target_rate': 100,
             'mode': 'smoke', 'execution_validity': 'ERROR', 'performance_stability': 'UNKNOWN', 'run_directory': 'r'}
        self.assertIsNone(flatten(r, {})['sink_throughput'])
        self.assertIsNone(flatten(r, {})['tm_cpu_quota_utilization'])

    def test_statistics_exclude_invalid_single_repeat_std_missing(self):
        try: import pandas
        except ImportError: self.skipTest('Pinned analyzer container runs this test; host pandas unavailable')
        base = {'run_id': 'r', 'role': 'fixed_input', 'parallelism': 1, 'target_rate': 100, 'mode': 'smoke',
                'execution_validity': 'VALID', 'performance_stability': 'UNKNOWN', 'run_directory': 'r',
                'observations': {'measurement_sink_throughput': 10}}
        rows = [flatten(base, {}), flatten(base | {'execution_validity': 'INVALID', 'observations': {'measurement_sink_throughput': 999}}, {})]
        result = next(r for r in repeat_statistics(rows) if r['metric'] == 'sink_throughput')
        self.assertEqual(result['mean'], 10); self.assertIsNone(result['std']); self.assertEqual(result['valid_runs'], 1)

    def test_statistics_do_not_mix_different_physical_budgets(self):
        try: import pandas
        except ImportError: self.skipTest('Requires pinned analyzer container')
        r = {'run_id': 'r', 'role': 'fixed_input', 'parallelism': 1, 'target_rate': 100, 'mode': 'smoke',
             'execution_validity': 'VALID', 'performance_stability': 'UNKNOWN', 'run_directory': 'r',
             'observations': {'measurement_sink_throughput': 10}}
        rows = [flatten(r, {'fingerprint': 'budget1'}), flatten(r, {'fingerprint': 'budget2'})]
        groups = [v for v in repeat_statistics(rows) if v['metric'] == 'sink_throughput']
        self.assertEqual(len(groups), 2); self.assertTrue(all(v['observed_runs'] == 1 for v in groups))


if __name__ == '__main__': unittest.main()
