"""Sequential isolated experiments 1 and 2 built on Phase 2 real run execution."""
import copy
import json
import os
import random
import time
import uuid
from pathlib import Path
from run_once import run
from result_store import RunStore
from phase3_config import cost_plan
from capacity_analysis import next_rate, bounds, search_verdict, diagnose, estimate_operator_capacity, stats
from calculate_parallelism import calculate, neighbors
from analyze_results import flatten, write_csv


def prediction(capacity, plan):
    if capacity['status'] != 'CONDITIONAL_ESTIMATE':
        return {'status': 'NOT_ESTIMATED', 'required_parallelism': None,
                'reason': 'No transferable measured operator capacity; Smoke has UNKNOWN stability', 'capacity': capacity}
    result = calculate(plan['peak_input_rate'], capacity['subtask_capacity'], plan['target_utilization'], capacity['operator'])
    result.update(status='CONDITIONAL_PREDICTION', capacity=capacity,
                  sensitivity=[calculate(plan['peak_input_rate'], capacity['subtask_capacity'], u, capacity['operator']) for u in [.6, .7, .8]])
    return result


def evaluate_validation(rows, diagnostics, predicted, policy, expected_repetitions):
    calibration_ids = set(predicted.get('calibration_run_ids', predicted['capacity']['source_run_ids']))
    validation_ids = {r['run_id'] for r in rows}
    if validation_ids & calibration_ids: raise ValueError('Calibration and validation Run IDs overlap')
    groups = {}
    for row in rows: groups.setdefault(row['parallelism'], []).append(row)
    minimum = None; operational = None; cases = []
    fingerprint = predicted['capacity']['entries'][0]['fingerprint'] if predicted['status'] == 'CONDITIONAL_PREDICTION' else None
    for p, repeats in sorted(groups.items()):
        # Any invalid, missing or conflicting repeat prevents claiming stable at P.
        stable = len(repeats) == expected_repetitions and all(r['execution_validity'] == 'VALID' and r['performance_stability'] == 'STABLE' for r in repeats)
        compatible = fingerprint is not None and all(diagnostics[r['run_id']]['fingerprint'] == fingerprint for r in repeats)
        eligible = stable and compatible
        if eligible and minimum is None: minimum = p
        operational_checks = []
        for r in repeats:
            o = r['observations']; d = diagnostics[r['run_id']]
            latency = o.get('latency', {}).get('p99'); cp = o.get('checkpoint_latest', {}).get('completed') or {}
            cpu = d['resources']['taskmanager']['cpu_quota_utilization']['max']; throttle = d['resources']['taskmanager']['throttled_period_ratio']['max']
            conservative_capacity = predicted['capacity'].get('subtask_capacity')
            requested_utilization = predicted.get('target_utilization')
            headroom = conservative_capacity is not None and requested_utilization is not None and predicted['peak_input_rate'] / (p * conservative_capacity) <= requested_utilization
            operational_checks.append(bool(latency is not None and latency <= policy['latency_p99_slo_ms'] and
                cp.get('end_to_end_duration') is not None and cp['end_to_end_duration'] <= policy['checkpoint_duration_slo_ms'] and
                cpu is not None and cpu <= policy['maximum_cpu_quota_utilization'] and
                throttle is not None and throttle <= policy['maximum_throttled_period_ratio'] and
                headroom and not d['resource_saturation'] and not d['resource_evidence_missing']))
        if eligible and all(operational_checks) and operational is None: operational = p
        cases.append({'parallelism': p, 'run_ids': [r['run_id'] for r in repeats], 'stable_repeats': sum(r['execution_validity'] == 'VALID' and r['performance_stability'] == 'STABLE' for r in repeats),
                      'all_repeats_stable': stable, 'conditions_match_calibration': compatible,
                      'operational_policy_met': eligible and all(operational_checks)})
    pstar = predicted['required_parallelism']
    return {'predicted_required_parallelism': pstar, 'measured_minimum_stable_parallelism': minimum,
            'recommended_operational_parallelism': operational,
            'calculation_error_parallelism': pstar - minimum if pstar is not None and minimum is not None else None,
            'calculation_relative_error': (pstar - minimum) / minimum if pstar is not None and minimum is not None else None,
            'cases': cases, 'calibration_run_ids': sorted(calibration_ids), 'validation_run_ids': sorted(validation_ids),
            'status': 'OBSERVED_WITHIN_TESTED_CANDIDATES' if minimum is not None else 'NOT_ESTIMATED',
            'limitations': ['Minimum is only among actually tested candidates; not a global minimum',
                            'Operating recommendation is conditional on registered SLO/resource/headroom policy',
                            'Smoke probes have no measured capacity prediction; do not label them calculated-P validation']}


def execute(config, experiment, output_dir, base='http://localhost:8081', runner=run, diagnostician=diagnose):
    session_id = 'session_' + uuid.uuid4().hex
    directory = Path(output_dir) / ('experiment_' + str(experiment)) / session_id
    store = RunStore(directory)
    config = copy.deepcopy(config); plan = config['plan']; mode = config['mode']
    rng = random.Random(plan['order_seed']); rows = []; diagnostics = {}; searches = {}; errors = []; skipped = []
    emit = store.jsonl('runs.jsonl'); transitions = store.jsonl('search.jsonl')
    store.json('session.json', {'schema_version': 1, 'session_id': session_id, 'experiment': experiment,
                              'mode': mode, 'started_wall_ms': time.time_ns() // 1000000,
                              'frozen_config': config, 'cost_plan': cost_plan(config, experiment)})
    predicted = None; validation = None
    def perform(p, rate, role, attempt, repeat):
        cfg = copy.deepcopy(config['run_config'])
        cfg['operator_parallelism'] = {op: p for op in cfg['operator_parallelism']}
        cfg['target_rate'] = rate
        cfg['seed'] += repeat + (100000 if role in ['validation', 'uncalibrated_peak_probe'] else 0)
        context = {'session_id': session_id, 'role': role, 'parallelism': p, 'target_rate': rate,
                   'attempt': attempt, 'repetition': repeat, 'calibration_run_ids': predicted.get('calibration_run_ids', predicted['capacity']['source_run_ids']) if predicted else []}
        print(json.dumps({'state': 'RUN_START', **context}), flush=True)
        result = runner(cfg, mode, base, directory / 'runs', experiment_id='experiment_' + str(experiment), context=context)
        row = {**result, **context, 'mode': mode, 'run_directory': os.path.relpath(result['result_directory'], directory)}
        rows.append(row); emit(row)
        diagnostics[row['run_id']] = diagnostician(Path(result['result_directory']), config['evidence_thresholds'])
        print(json.dumps({'state': 'RUN_END', 'run_id': row['run_id'], 'validity': row['execution_validity'], 'stability': row['performance_stability']}), flush=True)
        if result.get('cleanup_errors'): raise RuntimeError('Cleanup failed; refusing another run: ' + str(result['cleanup_errors']))
        if any('KeyboardInterrupt' in e for e in result.get('observations', {}).get('execution_errors', [])): raise KeyboardInterrupt()
        return row
    def discovery(parallelisms, role):
        order = list(parallelisms)
        if plan['random_order']: rng.shuffle(order)
        for p in order:
            attempts = []
            while (rate := next_rate(attempts, plan)) is not None:
                results = [perform(p, rate, role, len(attempts), repeat) for repeat in range(plan['repetitions'])]
                fingerprints = {diagnostics[r['run_id']]['fingerprint'] for r in results}
                previous_fingerprints = {f for a in attempts for f in a.get('condition_fingerprints', [])}
                compatible = len(fingerprints | previous_fingerprints) == 1
                attempt = {'rate': rate, 'verdict': search_verdict(results) if compatible else 'INCONCLUSIVE',
                           'condition_fingerprints': sorted(fingerprints), 'conditions_comparable': compatible,
                           'run_ids': [r['run_id'] for r in results],
                           'observed_produced_rates': [r.get('observations', {}).get('producer_validation', {}).get('actual_sent_rate') for r in results],
                           'observed_sink_rates': [r.get('observations', {}).get('measurement_sink_throughput') for r in results],
                           'observed_sustainable_throughputs': [r.get('sustainable_throughput') for r in results],
                           'statuses': [{'validity': r['execution_validity'], 'stability': r['performance_stability']} for r in results]}
                attempts.append(attempt); transitions({'parallelism': p, 'role': role, **attempt})
            searches[str(p)] = bounds(attempts, plan['rate_relative_resolution'])
    try:
        if experiment == 1:
            discovery(plan['parallelisms'], 'discovery')
            cases = [(p, repeat) for p in plan['parallelisms'] for repeat in range(plan['repetitions'])]
            if plan['random_order']: rng.shuffle(cases)
            for p, repeat in cases: perform(p, plan['fixed_input_rate'], 'fixed_input', 0, repeat)
        else:
            discovery(plan['calibration_parallelisms'], 'calibration')
            capacity = estimate_operator_capacity(searches, diagnostics, config['evidence_thresholds'])
            predicted = prediction(capacity, plan)
            predicted['calibration_run_ids'] = [r['run_id'] for r in rows if r['role'] == 'calibration']
            store.json('prediction.json', predicted)  # Immutable before independent validation begins.
            if predicted['status'] == 'CONDITIONAL_PREDICTION':
                candidates = neighbors(predicted['required_parallelism']); role = 'validation'
            elif mode == 'smoke':
                candidates = plan['uncalibrated_probe_parallelisms']; role = 'uncalibrated_peak_probe'
            else:
                candidates = []; role = 'validation'
                skipped.append({'stage': 'validation', 'reason': 'No evidence-qualified calibration; no invented P or capacity'})
            cases = []
            for p in candidates:
                if p > min(plan['max_test_parallelism'], config['run_config']['max_parallelism']):
                    skipped.append({'parallelism': p, 'reason': 'Exceeds configured slot/parallelism budget; Not Executed'})
                else: cases.extend((p, repeat) for repeat in range(plan['repetitions']))
            if plan['random_order']: rng.shuffle(cases)
            validation_rows = [perform(p, plan['peak_input_rate'], role, 0, repeat) for p, repeat in cases]
            validation = evaluate_validation(validation_rows, diagnostics, predicted, config['operational_policy'], plan['repetitions'])
    except (Exception, KeyboardInterrupt) as exc:
        errors.append(repr(exc))
    finally:
        status = 'ERROR' if errors else 'COMPLETED_WITH_RUN_FAILURES' if any(r['execution_validity'] in ['ERROR', 'INVALID'] for r in rows) else 'COMPLETED'
        summary = {'session_id': session_id, 'experiment': experiment, 'mode': mode, 'status': status,
                   'run_count': len(rows), 'run_ids': [r['run_id'] for r in rows], 'searches': searches,
                   'prediction': predicted, 'validation': validation, 'skipped': skipped, 'errors': errors,
                   'limitations': ['Smoke validates automation only; sustainable capacity remains unmeasured',
                                   'Fixed physical resource budget; logical P is not CPU allocation']}
        table = [flatten(r, diagnostics.get(r['run_id'], {})) for r in rows]
        write_csv(directory / 'summary.csv', table, list(table[0]) if table else ['run_id', 'role', 'parallelism'])
        store.json('diagnostics.json', diagnostics); store.json('session_summary.json', summary); store.close()
    print(json.dumps({'session_directory': str(directory), 'status': status, 'runs': len(rows)}), flush=True)
    return directory, summary
