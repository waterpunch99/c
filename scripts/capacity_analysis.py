"""Evidence gates: observed job bounds never become every operator's capacity."""
import hashlib
import json
import statistics
from pathlib import Path
from collect_metrics import metric, operator_key
from result_store import read_jsonl

OPERATORS = ['source', 'parse', 'heavy', 'aggregation', 'sink']


def stats(values):
    values = [v for v in values if v is not None]
    return {'count': len(values), 'mean': statistics.mean(values) if values else None,
            'std': statistics.stdev(values) if len(values) > 1 else None,
            'min': min(values) if values else None, 'max': max(values) if values else None}


def cv(values):
    return statistics.pstdev(values) / statistics.mean(values) if values and statistics.mean(values) > 0 else None


def measured_samples(directory, metadata):
    plan = metadata.get('plan')
    if not plan: return []
    return [s for s in read_jsonl(Path(directory) / 'raw_samples.jsonl')
            if s['started_wall_ms'] >= plan['measurement_start_wall_ms'] and s['wall_time_ms'] <= plan['measurement_end_wall_ms']]


def comparable_fingerprint(metadata):
    cfg = {k: v for k, v in metadata['config'].items() if k not in ['operator_parallelism', 'target_rate', 'seed']}
    env = metadata.get('environment', {})
    containers = [{k: c.get(k) for k in ['Image']} | {'limits': {k: c.get('HostConfig', {}).get(k) for k in
                   ['NanoCpus', 'CpuQuota', 'CpuPeriod', 'CpusetCpus', 'Memory']},
                   'service': c.get('Config', {}).get('Labels', {}).get('com.docker.compose.service')}
                  for c in env.get('docker_inspect', [])]
    value = {'config': cfg, 'containers': sorted(containers, key=lambda x: str(x['service'])),
             'host': env.get('host'), 'git': env.get('git_commit_sha'), 'artifacts': metadata.get('artifact_hashes'),
             'producer_images': [c.get('Image') for c in metadata.get('producer_container_inspect', [])],
             'docker_capacity': {k: env.get('docker_info', {}).get(k) for k in ['NCPU', 'MemTotal']},
             'runtime_versions': {k: env.get(k) for k in ['flink_java_version', 'kafka_version']},
             'flink_resources': {r['key']: r['value'] for r in metadata.get('flink_configuration', []) if
                                 r['key'].startswith(('taskmanager.memory.', 'jobmanager.memory.', 'metrics.fetcher.')) or
                                 r['key'] in ['taskmanager.numberOfTaskSlots', 'taskmanager.cpu.cores']}}
    # Transient host load is an observation, not a resource allocation.
    value['host'] = {k: v for k, v in (value['host'] or {}).items() if k != 'load_average'}
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def diagnose(directory, thresholds):
    directory = Path(directory)
    metadata = json.loads((directory / 'metadata.json').read_text())
    summary = json.loads((directory / 'summary.json').read_text())
    samples = measured_samples(directory, metadata)
    operators = {}
    for op in OPERATORS:
        per_subtask = {}
        for sample in samples:
            for row in sample['subtasks']:
                if operator_key(row['operator']) != op: continue
                per_subtask.setdefault(row['subtask'], []).append((sample['wall_time_ms'], row['metrics']))
        rates = []; busy = []; idle = []; pressure = []
        complete = len(per_subtask) == metadata['config']['operator_parallelism'][op]
        for subtask, rows in sorted(per_subtask.items()):
            counter = 'completedRecords' if op == 'sink' else 'numRecordsOut'
            values = [metric(m, counter) for _, m in rows]
            if len(rows) < 2 or any(v is None for v in values) or any(b < a for a, b in zip(values, values[1:])):
                rates.append(None); complete = False
            else:
                duration = (rows[-1][0] - rows[0][0]) / 1000
                rates.append((values[-1] - values[0]) / duration if duration > 0 else None)
            for name, target in [('busyTimeMsPerSecond', busy), ('idleTimeMsPerSecond', idle),
                                 ('backPressuredTimeMsPerSecond', pressure)]:
                vals = [metric(m, name) for _, m in rows]
                if not vals or any(v is None for v in vals): complete = False
                else: target.append(statistics.mean(vals))
        valid_rates = rates if rates and all(v is not None for v in rates) else []
        operators[op] = {'parallelism': metadata['config']['operator_parallelism'][op],
                         'subtask_output_rates': rates, 'observed_output_rate': sum(valid_rates) if valid_rates else None,
                         'observed_rate_per_subtask_mean': statistics.mean(valid_rates) if valid_rates else None,
                         'rate_cv': cv(valid_rates), 'busy_ms_per_second': statistics.mean(busy) if busy else None,
                         'idle_ms_per_second': statistics.mean(idle) if idle else None,
                         'backpressure_ms_per_second': statistics.mean(pressure) if pressure else None,
                         'metrics_complete': complete, 'capacity_per_subtask': None,
                         'capacity_status': 'NOT_ESTIMATED',
                         'reason': 'Observed rate at offered load is not saturation capacity'}
    candidates = []
    for op, row in operators.items():
        if not row['metrics_complete'] or row['rate_cv'] is None: continue
        others = [v['busy_ms_per_second'] for name, v in operators.items() if name != op]
        if any(v is None for v in others): continue
        if row['busy_ms_per_second'] >= thresholds['minimum_busy_ms_per_second'] and row['backpressure_ms_per_second'] <= thresholds['maximum_backpressure_ms_per_second'] and row['rate_cv'] <= thresholds['maximum_subtask_rate_cv'] and row['busy_ms_per_second'] - max(others) >= thresholds['minimum_busy_margin_ms_per_second']:
            candidates.append(op)
    resources = {service: {'cpu_quota_utilization': stats([s['resources'].get(service, {}).get('delta', {}).get('cpu_quota_utilization') for s in samples]),
                           'throttled_period_ratio': stats([s['resources'].get(service, {}).get('delta', {}).get('throttled_period_ratio') for s in samples])}
                 for service in ['taskmanager', 'kafka']}
    saturated = []
    for service, row in resources.items():
        for name, key in [('cpu_quota_utilization', 'maximum_cpu_quota_utilization'), ('throttled_period_ratio', 'maximum_throttled_period_ratio')]:
            if row[name]['max'] is not None and row[name]['max'] > thresholds[key]: saturated.append(service + '/' + name)
    obs = summary.get('observations', {})
    for row in obs.get('producer_resource_intervals', []):
        if row['cpu_quota_utilization'] is not None and row['cpu_quota_utilization'] > thresholds['maximum_cpu_quota_utilization']: saturated.append('producer/cpu_quota_utilization')
        if row['throttled_period_ratio'] is not None and row['throttled_period_ratio'] > thresholds['maximum_throttled_period_ratio']: saturated.append('producer/throttled_period_ratio')
    return {'run_id': summary['run_id'], 'mode': metadata['mode'], 'execution_validity': summary['execution_validity'],
            'performance_stability': summary['performance_stability'], 'fingerprint': comparable_fingerprint(metadata),
            'operators': operators, 'bottleneck_candidate': candidates[0] if len(candidates) == 1 else None,
            'candidate_reason': 'Busy, downstream pressure, load uniformity and other operator metrics checked; CPU causality unproven',
            'resources': resources, 'resource_saturation': sorted(set(saturated)),
            'resource_evidence_missing': obs.get('resource_evidence_missing', True),
            'measurement_samples': len(samples), 'config': metadata['config']}


def search_verdict(runs):
    if not runs or any(r['execution_validity'] != 'VALID' for r in runs): return 'INCONCLUSIVE'
    if all(r['performance_stability'] == 'STABLE' for r in runs): return 'STABLE'
    if all(r['performance_stability'] == 'UNSTABLE' and 'measured backlog growth' in r.get('reasons', []) for r in runs): return 'UNSTABLE'
    # Checkpoint failure without throughput saturation is not a capacity upper bound.
    return 'INCONCLUSIVE'


def bounds(attempts, resolution):
    stable = [a['rate'] for a in attempts if a['verdict'] == 'STABLE']
    unstable = [a['rate'] for a in attempts if a['verdict'] == 'UNSTABLE']
    low = max(stable, default=None); high = min(unstable, default=None)
    contradiction = low is not None and high is not None and low >= high
    measured = [v for a in attempts if a['rate'] == low and a['verdict'] == 'STABLE'
                for v in a.get('observed_sustainable_throughputs', []) if v is not None]
    return {'stable_input_lower_bound': low, 'backlog_unstable_input_upper_bound': high,
            'observed_sustainable_throughput_lower_bound': min(measured) if measured else None,
            'resolution_records_per_second': high - low if low is not None and high is not None and not contradiction else None,
            'relative_resolution_target': resolution,
            'status': 'CONTRADICTORY' if contradiction else 'BRACKETED' if low is not None and high is not None else 'RIGHT_CENSORED' if low is not None else 'NO_STABLE_LOWER_BOUND',
            'maximum_capacity': None, 'attempts': attempts,
            'limitations': ['Stable offered rate is the configured target within producer tolerance; actual produced/sink rates retained',
                            'Observed stable processing rate is a lower bound, not a proven maximum; all attempts preserved']}


def next_rate(attempts, plan):
    if len(attempts) >= plan['max_attempts']: return None
    if not attempts: return plan['initial_rate']
    if attempts[-1]['verdict'] == 'INCONCLUSIVE': return None
    b = bounds(attempts, plan['rate_relative_resolution']); low = b['stable_input_lower_bound']; high = b['backlog_unstable_input_upper_bound']
    if b['status'] == 'CONTRADICTORY': return None
    if low is not None and high is not None:
        if (high - low) / low <= plan['rate_relative_resolution']: return None
        return (low + high) / 2
    if low is None:
        return max(plan['minimum_rate'], high / plan['growth_factor']) if high is not None and high > plan['minimum_rate'] else None
    return min(plan['max_rate'], low * plan['growth_factor']) if low < plan['max_rate'] else None


def estimate_operator_capacity(searches, diagnostics, thresholds):
    entries = []; refusals = []
    for p, search in searches.items():
        low = search['stable_input_lower_bound']; high = search['backlog_unstable_input_upper_bound']
        if low is None or high is None or search['status'] != 'BRACKETED' or (high - low) / low > thresholds['maximum_capacity_bracket_relative_width']:
            refusals.append(f'P={p}: no sufficiently narrow stable/backlog-unstable bracket'); continue
        stable_ids = [rid for a in search['attempts'] if a['rate'] == low and a['verdict'] == 'STABLE' for rid in a['run_ids']]
        upper_ids = [rid for a in search['attempts'] if a['rate'] == high and a['verdict'] == 'UNSTABLE' for rid in a['run_ids']]
        rows = [diagnostics[r] for r in stable_ids + upper_ids]
        if not rows or any(r['mode'] != 'full' or r['execution_validity'] != 'VALID' for r in rows):
            refusals.append(f'P={p}: Full valid evidence required'); continue
        names = {r['bottleneck_candidate'] for r in rows}
        if len(names) != 1 or None in names or next(iter(names)) not in ['heavy', 'aggregation']:
            refusals.append(f'P={p}: unique CPU/stateful bottleneck not established (Source/Sink included)'); continue
        op = next(iter(names))
        if any(r['resource_saturation'] or r['resource_evidence_missing'] for r in rows):
            refusals.append(f'P={p}: physical/Producer/Kafka contention prevents transferable subtask estimate'); continue
        if len({r['fingerprint'] for r in rows}) != 1:
            refusals.append(f'P={p}: conditions/resources differ'); continue
        capacities = [diagnostics[r]['operators'][op]['observed_rate_per_subtask_mean'] for r in stable_ids]
        if any(v is None or v <= 0 for v in capacities):
            refusals.append(f'P={p}: missing operator output counters'); continue
        entries.append({'parallelism': int(p), 'operator': op, 'per_subtask_estimate': min(capacities),
                        'repeat_statistics': stats(capacities), 'source_run_ids': stable_ids, 'saturation_run_ids': upper_ids,
                        'fingerprint': rows[0]['fingerprint'], 'status': 'CONDITIONAL_LOWER_BOUND',
                        'assumptions': ['Same workload, balanced traffic, 1:1 output, isolated limits; capacity transfer needs independent validation']})
    names = {e['operator'] for e in entries}; fingerprints = {e['fingerprint'] for e in entries}
    values = [e['per_subtask_estimate'] for e in entries]
    dispersion = cv(values)
    accepted = len(entries) >= thresholds['minimum_calibration_parallelisms'] and len(names) == 1 and len(fingerprints) == 1 and dispersion is not None and dispersion <= thresholds['maximum_cross_parallelism_capacity_cv']
    if not accepted: refusals.append('Need coherent conditional estimates at multiple P; no job-throughput/P fallback')
    return {'status': 'CONDITIONAL_ESTIMATE' if accepted else 'NOT_ESTIMATED',
            'operator': next(iter(names)) if accepted else None,
            'subtask_capacity': min(values) if accepted else None, 'cross_parallelism_cv': dispersion,
            'entries': entries, 'refusals': refusals,
            'source_run_ids': sorted({rid for e in entries for rid in e['source_run_ids'] + e['saturation_run_ids']}),
            'limitations': ['Conditional conservative estimate; changing P can change contention even in fixed-resource mode']}
