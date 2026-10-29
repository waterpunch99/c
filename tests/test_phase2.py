"""Fixture-only regressions for Phase 2; no benchmark outputs are fabricated."""
import copy
import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from phase2_config import load_config
from measurement import regression, histogram_quantiles, counter_rate, classify, producer_decision, partition_imbalance
from collect_metrics import join_offsets, resource_delta, metric
from result_store import RunStore, read_jsonl
import run_once


def evidence():
    return {'producer_valid': True, 'preflight_valid': True, 'restart_count': 0, 'measurement_seconds': 180,
            'backlog_regression': regression([(i * 30, 0) for i in range(7)]),
            'latency_regression': regression([(i * 30, 15) for i in range(7)]),
            'checkpoint_completed': 10, 'checkpoint_failures': 0, 'clock_trusted': True,
            'latency_samples': 1000, 'sample_coverage': .95, 'measurement_sink_produced_ratio': .99,
            'measurement_sink_throughput': 198}


class MeasurementTest(unittest.TestCase):
    def setUp(self):
        self.cfg = load_config(); self.thresholds = self.cfg['thresholds']

    def test_regression_slope_units_and_uncertainty(self):
        r = regression([(1000 + i * 5, 100 + i * 50) for i in range(8)])
        self.assertAlmostEqual(r['slope'], 10)
        self.assertEqual(r['delta'], 350)
        self.assertAlmostEqual(r['standard_error'], 0)
        self.assertIsNone(regression([(0, None), (1, 0)])['slope'])

    def test_histogram_merge_preserves_job_quantiles_not_mean_subtask_p99(self):
        buckets = [0] * 64; buckets[1] = 1000; buckets[10] = 20
        h = histogram_quantiles(buckets)
        self.assertEqual(h['p50'], 1); self.assertEqual(h['p99'], 1023)
        self.assertEqual(h['sample_count'], 1020)
        self.assertIsNone(histogram_quantiles([None])['p99'])
        self.assertIsNone(histogram_quantiles([0] * 64)['p99'])

    def test_counter_reset_and_missing_not_zero(self):
        self.assertIsNone(counter_rate(None, 10, 5)); self.assertIsNone(counter_rate(10, 2, 5))
        self.assertEqual(counter_rate(10, 10, 5), 0)
        self.assertEqual(metric({'numRecordsOut': '10', 'Parse.numRecordsOut': '10'}, 'numRecordsOut'), 10)

    def test_valid_unstable_is_preserved(self):
        e = evidence(); e['backlog_regression'] = regression([(i * 30, i * 3000) for i in range(7)])
        r = classify(e, self.thresholds, 'full')
        self.assertEqual((r['execution_validity'], r['performance_stability']), ('VALID', 'UNSTABLE'))
        self.assertIsNone(r['sustainable_throughput'])

    def test_stable_load_is_lower_bound(self):
        r = classify(evidence(), self.thresholds, 'full')
        self.assertEqual(r['performance_stability'], 'STABLE'); self.assertEqual(r['sustainable_throughput'], 198)
        self.assertIn('observed load is a stable lower bound, not maximum capacity', r['limitations'])

    def test_smoke_does_not_claim_stable_capacity(self):
        r = classify(evidence(), self.thresholds, 'smoke')
        self.assertEqual((r['execution_validity'], r['performance_stability']), ('VALID', 'UNKNOWN'))
        self.assertIsNone(r['sustainable_throughput'])

    def test_invalid_producer_and_contamination(self):
        for key, value in [('producer_valid', False), ('preflight_valid', False), ('config_mismatch', True),
                           ('data_contamination', True), ('restart_count', 1)]:
            with self.subTest(key=key):
                e = evidence(); e[key] = value
                r = classify(e, self.thresholds, 'full')
                self.assertEqual((r['execution_validity'], r['performance_stability']), ('INVALID', 'UNKNOWN'))

    def test_required_evidence_or_clock_missing_is_inconclusive(self):
        for key, value in [('required_missing', ['read_offset']), ('restart_count', None), ('clock_trusted', False),
                           ('resource_evidence_missing', True), ('checkpoint_failures', None),
                           ('checkpoint_completed', None), ('latency_samples', None), ('measurement_cohort_unprocessed', 1)]:
            with self.subTest(key=key):
                e = evidence(); e[key] = value
                self.assertEqual(classify(e, self.thresholds, 'full')['execution_validity'], 'INCONCLUSIVE')

    def test_metric_failure_error_not_unstable(self):
        e = evidence(); e['metric_collection_failed'] = True
        self.assertEqual(classify(e, self.thresholds, 'full')['execution_validity'], 'ERROR')

    def test_noisy_backlog_does_not_claim_stable(self):
        e = evidence(); e['backlog_regression'] = regression([(i, 10000 if i % 2 else 0) for i in range(7)])
        self.assertEqual(classify(e, self.thresholds, 'full')['execution_validity'], 'INCONCLUSIVE')

    def test_noisy_latency_does_not_claim_stable(self):
        e = evidence(); e['latency_regression'] = regression([(i, 10000 if i % 2 else 0) for i in range(7)])
        self.assertEqual(classify(e, self.thresholds, 'full')['execution_validity'], 'INCONCLUSIVE')

    def test_partition_zero_mean_is_unknown(self):
        self.assertIsNone(partition_imbalance({}, 4)['max_mean_ratio'])
        self.assertEqual(partition_imbalance({'0': 100, '1': 100}, 2)['max_mean_ratio'], 1)

    def test_producer_ack_failure_rate_and_partition_validation(self):
        p = {'plan': {'measurement_seconds': 10, 'measurement_start_wall_ms': 0, 'measurement_end_wall_ms': 10000},
             'counts': {'measurement': {'sent': 2000, 'acked': 2000}}, 'acknowledged_partition_counts':
             {'measurement': {str(i): 250 for i in range(8)}}, 'pending_after_flush': 0}
        self.assertTrue(producer_decision(p, [], self.cfg)['valid'])
        p['counts']['measurement']['acked'] = 1999
        self.assertFalse(producer_decision(p, [], self.cfg)['valid'])
        p['counts']['measurement'] = {'sent': 1000, 'acked': 1000}
        self.assertIn('actual sent rate outside target tolerance', producer_decision(p, [], self.cfg)['reasons'])


class OffsetResourceTest(unittest.TestCase):
    def rows(self):
        return [{'operator': 'Source: Kafka Source', 'subtask': 0, 'metrics': {
            'KafkaSourceReader.sourceReadNextOffsetPartition0': '8', 'KafkaSourceReader.sourceReadNextOffsetPartition1': '10'}}]

    def test_leo_read_and_committed_lags_are_separate(self):
        kafka = {'wall_time_ms': 1000, 'partitions': [{'partition': 0, 'log_end_offset': 10, 'committed_offset': 5},
                                                   {'partition': 1, 'log_end_offset': 10, 'committed_offset': 4}]}
        r = join_offsets(self.rows(), kafka, 2, 1100, 1)
        self.assertEqual(r['read_backlog'], 2); self.assertEqual(r['committed_lag'], 11)
        self.assertEqual(r['partitions'][0]['source_subtask'], 0)

    def test_missing_stale_and_negative_backlog_not_clamped(self):
        kafka = {'wall_time_ms': 1000, 'partitions': [{'partition': 0, 'log_end_offset': 1, 'committed_offset': None}]}
        self.assertIsNone(join_offsets(self.rows(), kafka, 2, 1000, 1)['read_backlog'])
        self.assertIsNone(join_offsets(self.rows(), kafka, 2, 10000, 1)['read_backlog'])

    def test_cgroup_delta_uses_quota_not_slot_count(self):
        prev = {'wall_time_ms': 0, 'cpu.stat': {'usage_usec': 0, 'nr_periods': 0, 'nr_throttled': 0, 'throttled_usec': 0}}
        curr = {'wall_time_ms': 5000, 'cpu.max': '200000 100000', 'cpu.stat': {'usage_usec': 5000000,
                'nr_periods': 50, 'nr_throttled': 10, 'throttled_usec': 2000000}, 'memory.current': '100', 'memory.max': '200'}
        delta = resource_delta(prev, curr)
        self.assertEqual(delta['cpu_cores_used'], 1); self.assertEqual(delta['cpu_quota_utilization'], .5)
        self.assertEqual(delta['throttled_period_ratio'], .2)
        self.assertIsNone(resource_delta(None, curr)['cpu_cores_used'])


class StoreConfigTest(unittest.TestCase):
    def test_raw_result_exclusive_and_missing_csv_empty(self):
        with tempfile.TemporaryDirectory() as d:
            store = RunStore(Path(d) / 'run')
            append = store.csv('samples.csv', ['value']); append({'value': None}); store.close()
            with self.assertRaises(FileExistsError):
                RunStore(Path(d) / 'run')
            with (Path(d) / 'run/samples.csv').open() as f:
                self.assertEqual(list(csv.DictReader(f)), [{'value': ''}])

    def test_partial_jsonl_deferred_corruption_raises(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'raw.jsonl'; p.write_text('{"n":1}\n{"n":')
            self.assertEqual(read_jsonl(p), [{'n': 1}])
            p.write_text('broken\n')
            with self.assertRaises(ValueError): read_jsonl(p)

    def test_config_checks_and_phase_boundaries(self):
        for override in [{'target_rate': float('nan')}, {'producer_workers': 2}, {'chaining': True}, {'resource_mode': 'resource-scaled'}]:
            with self.assertRaises(ValueError): load_config(overrides=override)
        plan = {'start_wall_ms': 1000, 'measurement_start_wall_ms': 2000, 'measurement_end_wall_ms': 3000}
        self.assertEqual(run_once.phase_at(1999, plan), 'warmup')
        self.assertEqual(run_once.phase_at(2000, plan), 'measurement')
        self.assertEqual(run_once.phase_at(3000, plan), 'drain')

    def test_failed_preflight_prevents_job_submission_and_preserves_result(self):
        with tempfile.TemporaryDirectory() as d, patch.object(run_once, 'command', return_value=''), \
             patch.object(run_once, 'rest', side_effect=lambda base, path: {'jobs': []} if path == '/jobs/overview' else {'slots-available': 16}), \
             patch.object(run_once, 'environment', return_value={'clock_probes': {}}), \
             patch.object(run_once, 'preflight', return_value={'valid': False, 'reasons': ['fixture rate failure']}), \
             patch.object(run_once, 'cleanup_run', return_value=({}, [])), \
             patch.object(run_once, 'submit') as submit_mock, patch('builtins.print'):
            result = run_once.run(load_config(), 'smoke', 'http://fixture', Path(d))
            self.assertEqual(result['execution_validity'], 'INVALID')
            submit_mock.assert_not_called()
            self.assertTrue(Path(result['result_directory'], 'summary.json').exists())

    def test_drain_count_not_added_to_measurement_throughput(self):
        final = {'counts': {'measurement_cohort_completed': 100, 'measurement_window_cohort_completed': 80,
                          'measurement_window_completed': 85, 'parse_errors': 0, 'duplicates': 0},
                 'offsets': {'read_backlog': 0, 'committed_lag': 0}, 'restart_count': 0,
                 'checkpoints': {'counts': {'completed': 1, 'failed': 0}}, 'latency': {'sample_count': 10}, 'missing_metrics': {}}
        p = {'plan': {'measurement_seconds': 10, 'measurement_start_wall_ms': 1000, 'measurement_end_wall_ms': 11000},
             'counts': {'measurement': {'generated': 100, 'sent': 100, 'acked': 100}}, 'acknowledged_partition_counts': {'measurement': {}},
             'pending_after_flush': 0}
        cfg = load_config(overrides={'target_rate': 10, 'measurement_seconds': 10})
        o = run_once.observations([], final, p, [], {'valid': True}, cfg, 'smoke', {}, None, [])
        self.assertEqual(o['measurement_cohort_drain_completed'], 20)
        self.assertEqual(o['measurement_sink_throughput'], 8)

    def test_checkpoint_startup_failures_not_counted_as_measurement_failures(self):
        cfg = load_config(overrides={'target_rate': 10, 'measurement_seconds': 10})
        final = {'counts': {'measurement_cohort_completed': 100, 'measurement_window_cohort_completed': 100,
                          'measurement_window_completed': 100, 'parse_errors': 0, 'duplicates': 0},
                 'offsets': {'read_backlog': 0, 'committed_lag': 0}, 'restart_count': 0,
                 'checkpoints': {'counts': {'completed': 9, 'failed': 2}}, 'latency': {'sample_count': 10}, 'missing_metrics': {}}
        samples = []
        for wall, completed in [(2000, 2), (5000, 5), (9000, 9)]:
            s = copy.deepcopy(final)
            s.update(started_wall_ms=wall, wall_time_ms=wall, latency_interval={'p95': 15},
                     resources={}, actual_parallelism=cfg['operator_parallelism'])
            s['checkpoints']['counts']['completed'] = completed
            samples.append(s)
        p = {'plan': {'start_wall_ms': 0, 'measurement_seconds': 10, 'measurement_start_wall_ms': 1000, 'measurement_end_wall_ms': 11000},
             'counts': {'measurement': {'generated': 100, 'sent': 100, 'acked': 100}},
             'acknowledged_partition_counts': {'measurement': {}}, 'pending_after_flush': 0}
        o = run_once.observations(samples, final, p, [], {'valid': True}, cfg, 'smoke', {}, None, [])
        self.assertEqual(o['checkpoint_failures'], 0)
        self.assertEqual(o['checkpoint_failures_total'], 2)
        self.assertEqual(o['checkpoint_completed'], 7)


if __name__ == '__main__':
    unittest.main()
