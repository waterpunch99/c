"""Observed subtask load/placement and comparison premises; never infer incoming load from throughput."""
import copy
import json
import statistics
from pathlib import Path
from collect_metrics import metric, operator_key
from capacity_analysis import diagnose, comparable_fingerprint, measured_samples, cv, stats
from result_store import read_jsonl


def imbalance(values):
    if not values or any(v is None or v < 0 for v in values):
        return {'max_mean_ratio': None, 'cv': None, 'reason': 'Missing/invalid subtask observations'}
    mean = statistics.mean(values)
    return {'max_mean_ratio': max(values) / mean if mean > 0 else None, 'cv': cv(values),
            'reason': 'Mean is zero; ratio undefined' if mean == 0 else 'Processed load; not incoming key distribution'}


def counter_difference(rows, name):
    vals = [metric(r['metrics'], name) for _, r in rows]
    if len(vals) < 2 or any(v is None for v in vals) or any(b < a for a,b in zip(vals, vals[1:])): return None
    return vals[-1] - vals[0]


def input_validation(producer, cfg, thresholds):
    if not producer: return {'status': 'INCONCLUSIVE', 'reasons': ['No observed producer distribution']}
    counts = producer['counts']['measurement']; generated = counts.get('generated')
    hot = counts.get('hot'); ratio = hot / generated if hot is not None and generated else None
    expected = cfg['hot_key_ratio'] if cfg['distribution'] == 'skewed' else 1 / cfg['number_of_users']
    parts = producer.get('acknowledged_partition_counts', {}).get('measurement', {})
    values = [parts.get(str(i), parts.get(i, 0)) for i in range(cfg['kafka_partitions'])] if parts else []
    partition_load = imbalance(values)
    reasons = []
    missing = generated is None or generated < thresholds['minimum_distribution_records'] or ratio is None or not values
    invalid = ratio is not None and abs(ratio - expected) > thresholds['maximum_hot_ratio_absolute_error']
    invalid |= partition_load['max_mean_ratio'] is not None and partition_load['max_mean_ratio'] > cfg['thresholds']['partition_max_mean_ratio']
    if missing: reasons.append('Insufficient generated records or partition evidence')
    if invalid: reasons.append('Observed key/partition distribution outside registered tolerance')
    return {'status': 'INVALID' if invalid else 'INCONCLUSIVE' if missing else 'VALID', 'reasons': reasons,
            'generated_records': generated, 'generated_hot_records': hot, 'actual_hot_key_ratio': ratio,
            'sent_hot_records': counts.get('sent_hot'), 'acked_hot_records': counts.get('acked_hot'),
            'expected_hot_key_ratio': expected, 'partition_counts': values, 'partition_load': partition_load}


def phase4_diagnose(directory, thresholds):
    directory = Path(directory)
    result = diagnose(directory, thresholds)
    metadata = json.loads((directory / 'metadata.json').read_text()); cfg = metadata['config']
    summary = json.loads((directory / 'summary.json').read_text()); obs = summary.get('observations', {})
    samples = measured_samples(directory, metadata); all_samples = read_jsonl(directory / 'raw_samples.jsonl')
    final = all_samples[-1] if all_samples else None
    producer = json.loads((directory / 'producer.json').read_text()) if (directory / 'producer.json').exists() else None
    controlled = copy.deepcopy(metadata); controlled['config']['distribution'] = 'uniform'
    result['comparison_fingerprint'] = comparable_fingerprint(controlled)
    result['input_validation'] = input_validation(producer, cfg, thresholds)
    details = []; op_max_busy = {}; op_max_pressure = {}
    for op in ['source', 'parse', 'heavy', 'aggregation', 'sink']:
        busy_values = []; pressure_values = []
        for index in range(cfg['operator_parallelism'][op]):
            rows = [(s['wall_time_ms'], r) for s in samples for r in s['subtasks'] if operator_key(r['operator']) == op and r['subtask'] == index]
            end = next((r for r in (final or {}).get('subtasks', []) if operator_key(r['operator']) == op and r['subtask'] == index), None)
            duration = (rows[-1][0] - rows[0][0]) / 1000 if len(rows) > 1 else None
            out_count = counter_difference(rows, 'completedRecords' if op == 'sink' else 'numRecordsOut')
            in_count = counter_difference(rows, 'numRecordsIn')
            def mean(name):
                vals = [metric(r['metrics'], name) for _, r in rows]
                return statistics.mean(vals) if vals and all(v is not None for v in vals) else None
            busy = mean('busyTimeMsPerSecond'); pressure = mean('backPressuredTimeMsPerSecond')
            busy_values.append(busy); pressure_values.append(pressure)
            supported = end is not None and metric(end['metrics'], 'operatorCpuTimeSupported') == 1
            cpu_ns = counter_difference(rows, 'sampledOperatorCpuNanos') if supported else None
            cpu_count = counter_difference(rows, 'operatorCpuSampleCount') if supported else None
            fresh = end is not None and final is not None and metric(end['metrics'], 'aggregationMetricObservedWallMs') is not None and 0 <= final['wall_time_ms'] - metric(end['metrics'], 'aggregationMetricObservedWallMs') <= cfg['thresholds']['maximum_metric_age_seconds'] * 1000
            def agg(name): return metric(end['metrics'], name) if op == 'aggregation' and fresh else None
            details.append({'operator': op, 'subtask': index, 'parallelism': cfg['operator_parallelism'][op],
                 'taskmanager_id': end.get('taskmanager_id') if end else None,
                 'measurement_observation_seconds': duration, 'observed_input_records': in_count, 'observed_output_records': out_count,
                 'input_rate': in_count / duration if in_count is not None and duration else None,
                 'output_rate': out_count / duration if out_count is not None and duration else None,
                 'busy_ms_per_second': busy, 'idle_ms_per_second': mean('idleTimeMsPerSecond'), 'backpressure_ms_per_second': pressure,
                 'sampled_body_cpu_nanos': cpu_ns, 'cpu_sample_count': cpu_count,
                 'cpu_invalid_sample_count': counter_difference(rows, 'operatorCpuInvalidSamples') if supported else None,
                 'sampled_body_cpu_ns_per_record': cpu_ns / cpu_count if cpu_ns is not None and cpu_count else None,
                 'cpu_status': 'Available approximation' if cpu_ns is not None and cpu_count else 'Unsupported/Disabled/No samples/Missing',
                 'measurement_cohort_processed': agg('aggregationMeasurementRecords'),
                 'measurement_cohort_hot_processed': agg('aggregationMeasurementHotRecords'),
                 'measurement_window_cohort_processed': agg('aggregationMeasurementWindowRecords'),
                 'measurement_window_hot_processed': agg('aggregationMeasurementWindowHotRecords'),
                 'observed_hot_subtask': agg('hotKeyObservedSubtask'), 'observed_hot_key_group': agg('hotKeyObservedKeyGroup')})
        op_max_busy[op] = max(busy_values) if busy_values and all(v is not None for v in busy_values) else None
        op_max_pressure[op] = max(pressure_values) if pressure_values and all(v is not None for v in pressure_values) else None
    candidates = []
    for op, busy in op_max_busy.items():
        others = [v for k,v in op_max_busy.items() if k != op]
        if busy is not None and all(v is not None for v in others) and busy >= thresholds['minimum_busy_ms_per_second'] and op_max_pressure[op] is not None and op_max_pressure[op] <= thresholds['maximum_backpressure_ms_per_second'] and busy - max(others) >= thresholds['minimum_busy_margin_ms_per_second']:
            candidates.append(op)
    result['bottleneck_candidate'] = candidates[0] if len(candidates) == 1 else None
    result['candidate_reason'] = 'Max-subtask busy, low downstream pressure and margin checked; skew allowed. Candidate, not causal proof.'
    aggregation = [r for r in details if r['operator'] == 'aggregation']
    processed = [r['measurement_cohort_processed'] for r in aggregation]
    hot = [r['measurement_cohort_hot_processed'] for r in aggregation]
    identified = [r['subtask'] for r in aggregation if r['measurement_cohort_hot_processed'] is not None and r['measurement_cohort_hot_processed'] > 0 and r['observed_hot_subtask'] == r['subtask']]
    input_obs = result['input_validation']; acknowledged_hot = input_obs.get('acked_hot_records')
    complete = all(v is not None for v in processed + hot)
    sent = obs.get('measurement_sent_records')
    exact = complete and sent is not None and sum(processed) == sent and acknowledged_hot is not None and sum(hot) == acknowledged_hot and obs.get('restart_count') == 0
    placement_status = 'OBSERVED_SINGLE_SUBTASK' if exact and len(identified) == 1 else 'NO_HOT_EVENTS' if exact and sum(hot) == 0 else 'INCONCLUSIVE'
    if complete and len([v for v in hot if v > 0]) > 1: placement_status = 'CONTAMINATED_MULTIPLE_SUBTASKS'
    result.update(subtask_details=details, operator_max_busy=op_max_busy, operator_max_backpressure=op_max_pressure,
        aggregation_load={'processed_cohort': imbalance(processed), 'observed_output_rate': imbalance([r['output_rate'] for r in aggregation]),
                          'hot_subtasks': identified, 'placement_status': placement_status, 'cohort_counts_reconciled': exact,
                          'generated_hot_ratio': input_obs.get('actual_hot_key_ratio'),
                          'processed_hot_ratio': sum(hot) / sum(processed) if complete and sum(processed) else None},
        resource_efficiency=resource_efficiency(samples, obs, metadata.get('plan')))
    return result


def resource_efficiency(samples, obs, plan=None):
    # cgroup timestamp intervals wholly inside observed measurement samples only; no full-window extrapolation.
    cpu = []; memory = []; slots = []
    # REST wall_time is captured before cgroup collection; use actual cgroup timestamps for interval bounds.
    selected = [s for s in samples if s.get('resources', {}).get('taskmanager', {}).get('raw', {}).get('wall_time_ms') is not None and
        (plan is None or plan['measurement_start_wall_ms'] <= s['resources']['taskmanager']['raw']['wall_time_ms'] <= plan['measurement_end_wall_ms'])]
    for a,b in zip(selected, selected[1:]):
        ar = a.get('resources', {}).get('taskmanager', {}).get('raw', {})
        br = b.get('resources', {}).get('taskmanager', {}).get('raw', {})
        av = (ar.get('cpu.stat') or {}).get('usage_usec'); bv = (br.get('cpu.stat') or {}).get('usage_usec')
        cpu.append((bv-av)/1e6 if av is not None and bv is not None and bv >= av else None)
    for s in selected:
        memory.append(s.get('resources', {}).get('taskmanager', {}).get('delta', {}).get('memory_bytes'))
        slots.append(s.get('cluster_occupied_slots'))
    cost = sum(cpu) if cpu and all(v is not None for v in cpu) else None
    duration = (selected[-1]['resources']['taskmanager']['raw']['wall_time_ms']-selected[0]['resources']['taskmanager']['raw']['wall_time_ms'])/1000 if len(selected)>1 else None
    return {'taskmanager_observed_core_seconds': cost,
            'observed_interval_seconds': duration, 'cpu_snapshot_count': len(selected),
            'taskmanager_observed_mean_cpu_cores': cost / duration if cost is not None and duration is not None and duration > 0 else None,
            'memory_bytes': stats(memory), 'occupied_slots': stats(slots),
            'note': 'Process cgroup cost includes all tasks/GC/runtime; cgroup timestamp-bounded subset, not full-window or per-operator cost'}


def precheck_decision(rows, diagnostics, cfg):
    t = cfg['evidence_thresholds']; reasons = []; qualified = True
    for r in rows:
        d = diagnostics[r['run_id']]; o = r.get('observations', {})
        if r['execution_validity'] != 'VALID': reasons.append('Invalid precheck execution')
        ratio = o.get('measurement_sink_produced_ratio'); lag = o.get('backlog_regression', {})
        if ratio is None or ratio < t['precheck_minimum_sink_ratio']: reasons.append('Low-work precheck sink ratio insufficient')
        ci = lag.get('ci95')
        if not ci or ci[1] > cfg['run_config']['thresholds']['backlog_slope_records_per_second']: reasons.append('Precheck backlog growth cannot be excluded')
        for op in ['source','parse','sink']:
            busy = d['operator_max_busy'][op]
            if busy is None or busy >= t['minimum_busy_ms_per_second']: reasons.append(op + ' precheck saturation/missing evidence')
        if r['performance_stability'] != 'STABLE': qualified = False
    status = 'FAILED_OR_INCONCLUSIVE' if reasons or not rows else 'QUALIFIED_LOW_WORK_ONLY' if qualified and len(rows) == cfg['plan']['repetitions'] else 'PROVISIONAL_SMOKE_ONLY'
    return {'status': status, 'run_ids': [r['run_id'] for r in rows], 'reasons': sorted(set(reasons)),
            'limitations': ['Low CPU/state workload checks transport/parse/sink at offered rate; real focused case still requires measured bottleneck evidence', 'No automatic workload retuning or causal certification']}


def paired_partition_comparison(a, b, threshold):
    x = a['input_validation']; y = b['input_validation']
    if x['status'] == 'INVALID' or y['status'] == 'INVALID' or a['execution_validity'] == 'INVALID' or b['execution_validity'] == 'INVALID':
        return {'status': 'INVALID', 'total_variation': None, 'reason': 'Observed execution/key/partition premise violation'}
    if x['status'] != 'VALID' or y['status'] != 'VALID' or a['execution_validity'] != 'VALID' or b['execution_validity'] != 'VALID': return {'status': 'INCONCLUSIVE', 'total_variation': None, 'reason': 'Both runs need valid producer/execution evidence'}
    if a['config']['operator_parallelism'] != b['config']['operator_parallelism'] or a['config']['seed'] != b['config']['seed']:
        return {'status': 'INVALID', 'total_variation': None, 'reason': 'Paired runs require identical actual graph parallelism and seed'}
    if a['comparison_fingerprint'] != b['comparison_fingerprint']: return {'status': 'INVALID', 'total_variation': None, 'reason': 'Workload/resource/artifact mismatch beyond controlled distribution'}
    u = x['partition_counts']; v = y['partition_counts']
    if len(u) != len(v) or not sum(u) or not sum(v): return {'status': 'INCONCLUSIVE', 'total_variation': None, 'reason': 'Missing partition distribution'}
    tv = sum(abs(i/sum(u)-j/sum(v)) for i,j in zip(u,v))/2
    return {'status': 'VALID' if tv <= threshold else 'INVALID', 'total_variation': tv, 'threshold': threshold,
            'reason': 'Observed normalized Kafka partition ACK counts; key distribution evaluated separately'}
