"""Actual Job ID REST metrics, explicit source next-offsets and cgroup observations."""
import json
import re
import time
from lab import command, rest, numeric_metric, snapshot_subtasks, metric_query_paths
from measurement import histogram_quantiles, counter_rate


def metric(metrics, suffix):
    if suffix in metrics:
        return numeric_metric(metrics[suffix])  # task scope, one unchained vertex
    matches = [(k, numeric_metric(v)) for k, v in metrics.items() if k == suffix or k.endswith('.' + suffix)]
    return matches[0][1] if len(matches) == 1 else None


def operator_key(name):
    for key, label in [('source', 'Kafka Source'), ('parse', 'Parse'), ('heavy', 'Heavy CPU'),
                       ('aggregation', 'Stateful Aggregation'), ('sink', 'Measurement Sink')]:
        if label in name:
            return key
    return None


def total(rows, operator, name):
    values = [metric(r['metrics'], name) for r in rows if operator_key(r['operator']) == operator]
    return sum(values) if values and all(v is not None for v in values) else None


def latency_histogram(rows):
    sinks = [r for r in rows if operator_key(r['operator']) == 'sink']
    buckets = [total(rows, 'sink', 'e2eLatencyMsBucket' + str(i)) for i in range(64)]
    minima = [metric(r['metrics'], 'e2eLatencyMsMin') for r in sinks]
    maxima = [metric(r['metrics'], 'e2eLatencyMsMax') for r in sinks]
    minimum = min((v for v in minima if v is not None and v >= 0), default=None)
    maximum = max((v for v in maxima if v is not None and v >= 0), default=None)
    return histogram_quantiles(buckets, minimum, maximum)


def histogram_buckets(rows):
    return [total(rows, 'sink', 'e2eLatencyMsBucket' + str(i)) for i in range(64)]


def source_offsets(rows):
    observed = {}
    conflicts = []
    for r in rows:
        if operator_key(r['operator']) != 'source':
            continue
        for name, value in r['metrics'].items():
            match = re.search(r'(?:^|\.)sourceReadNextOffsetPartition(\d+)$', name)
            if match:
                partition = int(match.group(1)); number = numeric_metric(value)
                if partition in observed:
                    conflicts.append(partition)
                observed[partition] = {'next_offset': number if number is not None and number >= 0 else None,
                                       'source_subtask': r['subtask'], 'metric_id': name}
    return observed, conflicts


def join_offsets(rows, kafka, partitions, source_observed_ms, max_age_seconds):
    reads, conflicts = source_offsets(rows)
    admin = {r['partition']: r for r in (kafka or {}).get('partitions') or []}
    fresh = bool(kafka) and abs(source_observed_ms - kafka['wall_time_ms']) <= max_age_seconds * 1000
    values = []
    for partition in range(partitions):
        leo = admin.get(partition, {}).get('log_end_offset')
        committed = admin.get(partition, {}).get('committed_offset')
        read = reads.get(partition, {}).get('next_offset')
        # Inconsistent asynchronous snapshots are missing evidence, never clamped to zero.
        backlog = leo - read if fresh and leo is not None and read is not None and leo >= read and partition not in conflicts else None
        commit_lag = leo - committed if fresh and leo is not None and committed is not None and leo >= committed else None
        values.append({'partition': partition, 'log_end_offset': leo, 'source_next_offset': read,
                       'committed_offset': committed, 'read_backlog': backlog, 'committed_lag': commit_lag,
                       'source_subtask': reads.get(partition, {}).get('source_subtask'),
                       'assignment_evidence': reads.get(partition, {}).get('metric_id'),
                       'missing_reason': None if backlog is not None else 'Missing/uninitialized/conflicting/stale or LEO predates source snapshot'})
    lag = sum(r['read_backlog'] for r in values) if all(r['read_backlog'] is not None for r in values) else None
    commit_lag = sum(r['committed_lag'] for r in values) if all(r['committed_lag'] is not None for r in values) else None
    return {'partitions': values, 'read_backlog': lag, 'committed_lag': commit_lag,
            'admin_wall_time_ms': (kafka or {}).get('wall_time_ms'), 'source_wall_time_ms': source_observed_ms,
            'snapshot_skew_ms': source_observed_ms - kafka['wall_time_ms'] if kafka else None,
            'conflicts': conflicts}


def rest_metrics(base, path):
    ids = [r['id'] for r in rest(base, path)]
    rows = []
    for query in metric_query_paths(path, ids):
        rows.extend(rest(base, query))
    return {r['id']: r.get('value') for r in rows}


def container_resources(service):
    script = "for f in cpu.max cpu.stat memory.current memory.max memory.events; do printf '%s\\n' \"FILE:$f\"; if [ -r /sys/fs/cgroup/$f ]; then cat /sys/fs/cgroup/$f; else printf 'MISSING\\n'; fi; done"
    raw = command(['docker', 'compose', 'exec', '-T', service, 'sh', '-c', script], timeout=15)
    result = {'wall_time_ms': time.time_ns() // 1000000, 'missing': {}}
    for section in raw.split('FILE:')[1:]:
        name, _, content = section.partition('\n'); content = content.strip()
        if content == 'MISSING':
            result[name] = None; result['missing'][name] = 'Unsupported cgroup v2 file'
        elif name in ['cpu.stat', 'memory.events']:
            result[name] = {k: int(v) for k, v in (line.split() for line in content.splitlines())}
        else:
            result[name] = content
    return result


def resource_delta(previous, current):
    result = {'cpu_cores_used': None, 'cpu_quota_utilization': None, 'throttled_period_ratio': None,
              'throttled_seconds': None, 'memory_bytes': None, 'memory_limit_bytes': None, 'oom_kill_count': None}
    if current.get('memory.current') is not None:
        result['memory_bytes'] = int(current['memory.current'])
    if current.get('memory.max') not in [None, 'max']:
        result['memory_limit_bytes'] = int(current['memory.max'])
    result['oom_kill_count'] = (current.get('memory.events') or {}).get('oom_kill')
    if not previous or not current.get('cpu.stat') or not previous.get('cpu.stat'):
        return result
    seconds = (current['wall_time_ms'] - previous['wall_time_ms']) / 1000
    cores = counter_rate(previous['cpu.stat'].get('usage_usec'), current['cpu.stat'].get('usage_usec'), seconds)
    result['cpu_cores_used'] = cores / 1e6 if cores is not None else None
    quota = current.get('cpu.max')
    if quota and not quota.startswith('max') and result['cpu_cores_used'] is not None:
        q, period = map(int, quota.split()); result['cpu_quota_utilization'] = result['cpu_cores_used'] / (q / period)
    if all(name in data for name in ['nr_periods', 'nr_throttled'] for data in [previous['cpu.stat'], current['cpu.stat']]):
        periods = current['cpu.stat']['nr_periods'] - previous['cpu.stat']['nr_periods']
        throttled = current['cpu.stat']['nr_throttled'] - previous['cpu.stat']['nr_throttled']
        result['throttled_period_ratio'] = throttled / periods if periods > 0 and throttled >= 0 else None
    throttle_us = counter_rate(previous['cpu.stat'].get('throttled_usec'), current['cpu.stat'].get('throttled_usec'), 1)
    result['throttled_seconds'] = throttle_us / 1e6 if throttle_us is not None else None
    return result


class FlinkCollector:
    def __init__(self, base, job_id, cfg):
        self.base = base; self.job_id = job_id; self.cfg = cfg; self.previous = None; self.previous_resources = {}

    def sample(self, kafka=None):
        begin = time.time_ns() // 1000000
        job = rest(self.base, '/jobs/' + self.job_id)
        if job['state'] != 'RUNNING':
            raise RuntimeError('Job left RUNNING: ' + job['state'])
        rows, _ = snapshot_subtasks(self.base, self.job_id, job['vertices'])
        observed = time.time_ns() // 1000000
        if callable(kafka):
            kafka = kafka()
        checkpoints = rest(self.base, '/jobs/' + self.job_id + '/checkpoints')
        job_metrics = rest_metrics(self.base, '/jobs/' + self.job_id + '/metrics')
        overview = rest(self.base, '/overview')
        tms = rest(self.base, '/taskmanagers')['taskmanagers']
        tm_metrics = {tm['id']: rest_metrics(self.base, '/taskmanagers/' + tm['id'] + '/metrics') for tm in tms}
        resources = {}
        for service in ['taskmanager', 'kafka']:
            raw = container_resources(service)
            resources[service] = {'raw': raw, 'delta': resource_delta(self.previous_resources.get(service), raw)}
            self.previous_resources[service] = raw
        counts = {key: total(rows, op, name) for key, op, name in [
            ('source_records', 'source', 'numRecordsOut'), ('sink_completed', 'sink', 'completedRecords'),
            ('measurement_cohort_completed', 'sink', 'measurementCompletedRecords'),
            ('measurement_window_completed', 'sink', 'measurementWindowCompletedRecords'),
            ('measurement_window_cohort_completed', 'sink', 'measurementWindowCohortCompletedRecords'),
            ('warmup_completed', 'sink', 'warmupCompletedRecords'), ('parse_errors', 'parse', 'parseErrors'),
            ('duplicates', 'sink', 'duplicateRecords'), ('negative_latency_samples', 'sink', 'negativeLatencySamples')]}
        missing = {k: 'Missing/ambiguous/unregistered metric' for k, v in counts.items() if v is None}
        timing = (observed - self.previous['wall_time_ms']) / 1000 if self.previous else None
        rates = {name: counter_rate(self.previous['counts'].get(key) if self.previous else None, counts[key], timing or 0)
                 for name, key in [('source_read_rate', 'source_records'), ('sink_completed_rate', 'sink_completed')]}
        restart = metric(job_metrics, 'fullRestarts')
        # Attempt numbers provide observed restart evidence even if the optional Job metric is absent.
        attempts = [s.get('attempt') for v in job['vertices'] for s in rest(self.base, f"/jobs/{self.job_id}/vertices/{v['id']}")['subtasks']]
        if restart is None and attempts and all(v is not None for v in attempts):
            restart = max(attempts)
        actual_config = {operator_key(v['name']): v['parallelism'] for v in job['vertices']}
        buckets = histogram_buckets(rows)
        previous_buckets = self.previous.get('latency_buckets') if self.previous else None
        delta_buckets = [c - p if c is not None and p is not None and c >= p else None for c, p in zip(buckets, previous_buckets)] if previous_buckets else None
        source_stamps = [metric(r['metrics'], 'sourceMetricSnapshotTimestampMs') for r in rows if operator_key(r['operator']) == 'source']
        sink_stamps = [metric(r['metrics'], 'sinkMetricSnapshotTimestampMs') for r in rows if operator_key(r['operator']) == 'sink']
        source_stamp = min(source_stamps) if source_stamps and all(v is not None for v in source_stamps) else None
        sink_stamp = min(sink_stamps) if sink_stamps and all(v is not None for v in sink_stamps) else None
        if source_stamp is None or observed - source_stamp > self.cfg['thresholds']['maximum_metric_age_seconds'] * 1000:
            missing['source_metric_snapshot'] = 'Missing/stale metric timestamp; backlog cannot establish stability'
        if sink_stamp is None or observed - sink_stamp > self.cfg['thresholds']['maximum_metric_age_seconds'] * 1000:
            missing['sink_metric_snapshot'] = 'Missing/stale metric timestamp'
        joined = join_offsets(rows, kafka, self.cfg['kafka_partitions'], source_stamp if source_stamp is not None else observed,
                              self.cfg['thresholds']['maximum_metric_age_seconds'])
        if 'source_metric_snapshot' in missing:
            joined['read_backlog'] = None
        sample = {'started_wall_ms': begin, 'wall_time_ms': observed, 'job_state': job['state'], 'counts': counts,
                  'rates': rates, 'latency': latency_histogram(rows), 'checkpoints': checkpoints,
                  'latency_buckets': buckets, 'latency_interval': histogram_quantiles(delta_buckets),
                  'restart_count': restart, 'job_metrics': job_metrics, 'taskmanager_metrics': tm_metrics,
                  'resources': resources, 'offsets': joined,
                  'source_metric_age_ms': observed - source_stamp if source_stamp is not None else None,
                  'sink_metric_age_ms': observed - sink_stamp if sink_stamp is not None else None,
                  'pending_records': total(rows, 'source', 'pendingRecords'),
                  'slots_total': overview['slots-total'], 'slots_available': overview['slots-available'],
                  'cluster_occupied_slots': overview['slots-total'] - overview['slots-available'],
                  'actual_parallelism': actual_config, 'subtasks': rows, 'missing_metrics': missing,
                  'collection_seconds': (time.time_ns() // 1000000 - begin) / 1000}
        for optional, reason in {'subtask_cpu_usage': 'Unsupported; Busy Time is not CPU',
                                 'state_size': 'Unsupported REST state-size gauge; checkpoint state_size retained',
                                 'pending_records': 'Source pendingRecords not registered'}.items():
            if optional != 'pending_records' or sample['pending_records'] is None:
                missing[optional] = reason
        self.previous = sample
        return sample
