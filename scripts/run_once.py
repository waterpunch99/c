"""Phase 2: preflight plus one isolated instrumented run; no experiment 1..5 orchestration."""
import argparse
import hashlib
import json
import math
import os
import platform
import signal
import subprocess
import time
import uuid
from pathlib import Path
from lab import ROOT, command, rest, wait_for, audit_sink
from submit_job import submit, wait_running
from smoke_test import cleanup_run, clock_probe
from preflight import preflight, create_topic, session_args
from phase2_config import load_config
from collect_metrics import FlinkCollector, operator_key, metric, resource_delta
from measurement import producer_decision, regression, classify
from result_store import RunStore, read_jsonl
from probe_transport import query_json


def environment(base):
    result = {'host': {'os': platform.platform(), 'architecture': platform.machine(),
                      'python': platform.python_version(), 'cpu_total': os.cpu_count(),
                      'cpu_affinity_count': len(os.sched_getaffinity(0)), 'load_average': os.getloadavg()},
              'compose': json.loads(command(['docker', 'compose', 'config', '--format', 'json'])),
              'flink_overview': rest(base, '/overview'), 'taskmanagers': rest(base, '/taskmanagers'),
              'flink_java_version': command(['docker', 'compose', 'exec', '-T', 'jobmanager', 'java', '-version']),
              'kafka_version': command(['docker', 'compose', 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--version']),
              'clock_probes': {name: clock_probe(name) for name in ['jobmanager', 'taskmanager']}}
    for tm in result['taskmanagers']['taskmanagers']:
        tm['details'] = rest(base, '/taskmanagers/' + tm['id'])
    ids = command(['docker', 'compose', 'ps', '-q', 'kafka', 'jobmanager', 'taskmanager']).split()
    result['docker_inspect'] = json.loads(command(['docker', 'inspect', *ids]))
    result['docker_info'] = json.loads(command(['docker', 'info', '--format', '{{json .}}']))
    try:
        result['git_commit_sha'] = command(['git', 'rev-parse', '--verify', 'HEAD']).strip()
    except RuntimeError:
        result['git_commit_sha'] = None
    result['git_status'] = command(['git', 'status', '--short'])
    result['missing_metadata'] = {'maven_version': 'Build evidence in docs/validation.md; builder not started during measurement'}
    return result


def producer_clock(name):
    before = time.time_ns() // 1000000
    value = int(command(['docker', 'exec', name, 'python', '-c', 'import time; print(time.time_ns()//1000000)']).strip())
    after = time.time_ns() // 1000000
    return {'container_minus_host_ms_lower': value - after, 'container_minus_host_ms_upper': value - before,
            'round_trip_ms': after - before}


def job_args(cfg, topic, group, run_id, verification, experiment_id='phase2'):
    args = ['--topic', topic, '--group-id', group, '--experiment-id', experiment_id, '--run-id', run_id,
            '--cpu-work-iterations', str(cfg['cpu_work_iterations']), '--keyed-cpu-work-iterations', str(cfg['keyed_cpu_work_iterations']),
            '--checkpoint-interval-ms', str(cfg['checkpoint_interval_ms']), '--checkpoint-timeout-ms', str(cfg['checkpoint_timeout_ms']),
            '--state-backend', cfg['state_backend'], '--max-parallelism', str(cfg['max_parallelism']), '--chaining', str(cfg['chaining']).lower(),
            '--latency-sample-every', str(cfg['latency_sample_every']), '--verification-records', str(verification)]
    for name, p in cfg['operator_parallelism'].items():
        args.extend(['--' + name + '-parallelism', str(p)])
    args.extend(['--operator-cpu-sample-every', str(cfg.get('operator_cpu_sample_every', 0))])
    return args


def phase_at(wall_ms, plan):
    if not plan or wall_ms < plan['start_wall_ms']:
        return 'ready'
    if wall_ms < plan['measurement_start_wall_ms']:
        return 'warmup'
    return 'measurement' if wall_ms < plan['measurement_end_wall_ms'] else 'drain'


def observations(samples, final, producer, telemetry, preflight_result, cfg, mode, clocks, audit, errors):
    if final is None or producer is None:
        return {'execution_errors': errors or ['missing final measurements'], 'producer_valid': False}
    plan = producer['plan']
    measured = [s for s in samples if s['started_wall_ms'] >= plan['measurement_start_wall_ms']
                and s['wall_time_ms'] <= plan['measurement_end_wall_ms']]
    lag = regression([(s['wall_time_ms'] / 1000, s['offsets']['read_backlog']) for s in measured])
    latency = regression([(s['wall_time_ms'] / 1000, s['latency_interval']['p95']) for s in measured])
    decision = producer_decision(producer, telemetry, cfg)
    counts = producer['counts']['measurement']
    generated = counts.get('generated', 0); sent = counts.get('sent', 0)
    completed = final['counts']['measurement_cohort_completed']
    window = final['counts']['measurement_window_cohort_completed']
    missing = [k for k, v in final['counts'].items() if v is None]
    if completed is None or final['offsets']['read_backlog'] is None:
        missing.append('final cohort count/read backlog')
    missing.extend(k for k in final['missing_metrics'] if k in ['source_metric_snapshot', 'sink_metric_snapshot'])
    trusted = bool(clocks) and all(max(abs(c['container_minus_host_ms_lower']), abs(c['container_minus_host_ms_upper'])) <=
                                cfg['thresholds']['maximum_clock_uncertainty_ms'] for c in clocks.values())
    resource_warnings = ['Producer/Kafka/Flink share host CPU; logical Flink ResourceProfile is not a physical allocation']
    resource_missing = False
    for s in measured[1:]:
        for name, r in s['resources'].items():
            delta = r['delta']
            if delta['cpu_quota_utilization'] is None or delta['throttled_period_ratio'] is None:
                resource_missing = True
            if delta['cpu_quota_utilization'] is not None and delta['cpu_quota_utilization'] > cfg['thresholds']['maximum_cpu_quota_utilization']:
                resource_warnings.append(name + ' CPU quota saturated in measured interval')
            if delta['throttled_period_ratio'] is not None and delta['throttled_period_ratio'] > cfg['thresholds']['maximum_throttled_period_ratio']:
                resource_warnings.append(name + ' CPU throttling exceeds registered warning threshold')
    producer_resource_intervals = []
    previous_resource = None
    for row in telemetry:
        current = row.get('resource')
        if current and previous_resource and plan['measurement_start_wall_ms'] <= previous_resource['wall_time_ms'] and current['wall_time_ms'] <= plan['measurement_end_wall_ms']:
            delta = resource_delta(previous_resource, current)
            producer_resource_intervals.append({'wall_time_ms': current['wall_time_ms'], **delta})
            if delta['cpu_quota_utilization'] is None or delta['throttled_period_ratio'] is None:
                resource_missing = True
            if delta['cpu_quota_utilization'] is not None and delta['cpu_quota_utilization'] > cfg['thresholds']['maximum_cpu_quota_utilization']:
                resource_warnings.append('producer CPU quota saturated in measured interval')
            if delta['throttled_period_ratio'] is not None and delta['throttled_period_ratio'] > cfg['thresholds']['maximum_throttled_period_ratio']:
                resource_warnings.append('producer CPU throttling exceeds registered warning threshold')
        if current:
            previous_resource = current
    warmup = [s for s in samples if phase_at(s['wall_time_ms'], plan) == 'warmup']
    ck = final['checkpoints']['counts']
    completed_values = [s['checkpoints']['counts'].get('completed') for s in measured]
    checkpoint_completed = completed_values[-1] - completed_values[0] if len(completed_values) >= 2 and all(v is not None for v in completed_values) else None
    failed_values = [s['checkpoints']['counts'].get('failed') for s in measured]
    checkpoint_failed = failed_values[-1] - failed_values[0] if len(failed_values) >= 2 and all(v is not None for v in failed_values) else None
    if checkpoint_failed is None:
        missing.append('checkpoint failure count')
    return {'execution_errors': errors, 'producer_valid': decision['valid'], 'producer_validation': decision,
            'preflight_valid': preflight_result['valid'], 'config_mismatch': any(s['actual_parallelism'] != cfg['operator_parallelism'] for s in samples),
            'data_contamination': bool((final['counts']['parse_errors'] or 0) or (final['counts']['duplicates'] or 0) or (audit and audit.get('error')) or (completed is not None and completed > sent)),
            'restart_count': final['restart_count'], 'required_missing': missing,
            'measurement_seconds': cfg['measurement_seconds'], 'measurement_generated_records': generated,
            'measurement_sent_records': sent, 'measurement_cohort_final_completed': completed,
            'measurement_cohort_completed_within_window': window,
            'measurement_window_total_completed': final['counts']['measurement_window_completed'],
            'measurement_cohort_drain_completed': completed - window if completed is not None and window is not None else None,
            'measurement_cohort_unprocessed': sent - completed if completed is not None else None,
            'cohort_completion_fraction': completed / sent if completed is not None and sent else None,
            'latency_cohort_complete': completed == sent if completed is not None else False,
            'measurement_sink_throughput': window / cfg['measurement_seconds'] if window is not None else None,
            'measurement_sink_produced_ratio': window / sent if window is not None and sent else None,
            'backlog_regression': {**lag, 'unit': 'records/sec', 'threshold': cfg['thresholds']['backlog_slope_records_per_second']},
            'warmup_final_observed_backlog': warmup[-1]['offsets']['read_backlog'] if warmup else None,
            'final_read_backlog': final['offsets']['read_backlog'], 'final_committed_lag': final['offsets']['committed_lag'],
            'latency_regression': {**latency, 'unit': 'ms/sec'}, 'latency': final['latency'],
            'latency_samples': final['latency']['sample_count'], 'clock_trusted': trusted, 'clock_evidence': clocks,
            'checkpoint_completed': checkpoint_completed, 'checkpoint_completed_total': ck.get('completed'),
            'checkpoint_failures': checkpoint_failed, 'checkpoint_failures_total': ck.get('failed'),
            'checkpoint_latest': final['checkpoints'].get('latest'), 'audit': audit,
            'sample_coverage': (measured[-1]['wall_time_ms'] - measured[0]['wall_time_ms']) / 1000 / cfg['measurement_seconds'] if len(measured) > 1 else 0,
            'measurement_sample_count': len(measured), 'resource_evidence_missing': resource_missing,
            'resource_warnings': sorted(set(resource_warnings)),
            'producer_resource_intervals': producer_resource_intervals, 'missing_metrics': final['missing_metrics']}


def run(cfg, mode, base, output_dir, experiment_id='phase2', context=None):
    run_id = 'run_' + uuid.uuid4().hex
    store = RunStore(output_dir / run_id)
    topic = 'lab-' + run_id.replace('_', '-')
    group = 'group-' + run_id
    producer_name = 'producer-' + run_id
    probe_name = 'probe-' + run_id
    observer_name = 'observer-' + run_id
    job_id = None; created = False; process = None; log = None; plan = None
    samples = []; errors = []; final = None; producer = None; audit = None; clocks = {}
    metadata = {'run_id': run_id, 'experiment_id': experiment_id, 'run_context': context, 'mode': mode, 'config': cfg, 'topic': topic,
                'group_id': group, 'checkpoint_path': 'file:///checkpoints/' + run_id,
                'verification_scope': 'bounded exact eventIds if planned total<=10000, no restart; otherwise counters only',
                'limitations': ['Smoke is not capacity evidence', 'sampled operator body CPU is not total subtask CPU usage', 'single producer worker; no resource-scaled mode']}
    metadata['artifact_hashes'] = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in [ROOT / 'flink-job/target/flink-parallelism-job.jar',
                                             *sorted((ROOT / 'load-generator').glob('*.py')),
                                             *sorted((ROOT / 'scripts').glob('*.py'))] if p.is_file()}
    preflight_result = {'valid': False, 'reasons': ['Not Executed']}
    events = store.jsonl('lifecycle.jsonl'); raw = store.jsonl('raw_samples.jsonl')
    kafka_queries = store.jsonl('kafka_queries.jsonl')
    probe_diagnostics = store.jsonl('kafka_probe_diagnostics.jsonl')
    sample_fields = ['wall_time_ms', 'phase', 'target_rate', 'producer_sent_rate', 'producer_ack_rate', 'source_read_rate', 'sink_completed_rate',
                     'source_records', 'sink_completed', 'measurement_cohort_completed', 'read_backlog', 'committed_lag', 'pending_records',
                     'latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms', 'latency_samples', 'checkpoint_completed', 'checkpoint_failed',
                     'restart_count', 'cluster_occupied_slots', 'taskmanager_cpu_quota_utilization', 'taskmanager_throttled_period_ratio',
                     'taskmanager_memory_bytes', 'collection_seconds']
    csv_sample = store.csv('samples.csv', sample_fields)
    csv_subtask = store.csv('subtasks.csv', ['wall_time_ms', 'phase', 'operator', 'parallelism', 'subtask', 'taskmanager_id',
                                           'metric', 'value', 'status', 'missing_reason'])
    events({'state': 'CREATED', 'wall_time_ms': time.time_ns() // 1000000})
    def collect(collector):
        kafka_rows = read_jsonl(store.directory / 'kafka.jsonl')
        kafka = kafka_rows[-1] if kafka_rows else None
        if process is not None and process.poll() is None:
            def query():
                try:
                    result = query_json(['docker', 'exec', producer_name, 'python', '/app/kafka_probe.py',
                        '--topic', topic, '--group', group, '--partitions', str(cfg['kafka_partitions'])], timeout=20,
                        attempts=cfg.get('kafka_probe_attempts',2),diagnostics=probe_diagnostics)
                except RuntimeError as probe_error:
                    # The --rm container can exit between process.poll() and docker exec.
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        raise probe_error
                    completed = read_jsonl(store.directory / 'kafka.jsonl')
                    if process.returncode != 0 or not completed:
                        raise
                    result = completed[-1]
                kafka_queries(result)
                return result
            kafka = query
        elif process is not None:
            def final_query():
                result = query_json(['docker', 'compose', 'run', '--rm', '--no-deps', '--name', observer_name,
                    '--entrypoint', 'python', 'producer', '/app/kafka_probe.py', '--topic', topic, '--group', group,
                    '--partitions', str(cfg['kafka_partitions'])], timeout=25,
                    attempts=cfg.get('kafka_probe_attempts',2),diagnostics=probe_diagnostics)
                kafka_queries(result)
                return result
            kafka = final_query
        s = collector.sample(kafka)
        telemetry = read_jsonl(store.directory / 'producer.jsonl')
        p = telemetry[-1] if telemetry else None
        s['phase'] = phase_at(s['wall_time_ms'], plan); s['producer_telemetry'] = p
        s['host_load_average'] = os.getloadavg()
        raw(s); samples.append(s)
        tm = s['resources']['taskmanager']['delta']
        csv_sample({'wall_time_ms': s['wall_time_ms'], 'phase': s['phase'], 'target_rate': cfg['target_rate'],
                    'producer_sent_rate': p['rates']['measurement']['sent'] if p else None,
                    'producer_ack_rate': p['rates']['measurement']['acked'] if p else None,
                    **s['rates'], 'source_records': s['counts']['source_records'], 'sink_completed': s['counts']['sink_completed'],
                    'measurement_cohort_completed': s['counts']['measurement_cohort_completed'],
                    'read_backlog': s['offsets']['read_backlog'], 'committed_lag': s['offsets']['committed_lag'],
                    'pending_records': s['pending_records'], 'latency_p50_ms': s['latency']['p50'],
                    'latency_p95_ms': s['latency']['p95'], 'latency_p99_ms': s['latency']['p99'], 'latency_samples': s['latency']['sample_count'],
                    'checkpoint_completed': s['checkpoints']['counts'].get('completed'), 'checkpoint_failed': s['checkpoints']['counts'].get('failed'),
                    'restart_count': s['restart_count'], 'cluster_occupied_slots': s['cluster_occupied_slots'],
                    'taskmanager_cpu_quota_utilization': tm['cpu_quota_utilization'], 'taskmanager_throttled_period_ratio': tm['throttled_period_ratio'],
                    'taskmanager_memory_bytes': tm['memory_bytes'], 'collection_seconds': s['collection_seconds']})
        for row in s['subtasks']:
            expected = ['numRecordsIn', 'numRecordsOut', 'numRecordsInPerSecond', 'numRecordsOutPerSecond',
                        'busyTimeMsPerSecond', 'idleTimeMsPerSecond', 'backPressuredTimeMsPerSecond']
            for name, value in row['metrics'].items():
                number = numeric(value)
                csv_subtask({'wall_time_ms': s['wall_time_ms'], 'phase': s['phase'], 'operator': row['operator'],
                             'parallelism': row['parallelism'], 'subtask': row['subtask'], 'taskmanager_id': row['taskmanager_id'],
                             'metric': name, 'value': number, 'status': 'Available' if number is not None else 'Missing',
                             'missing_reason': None if number is not None else 'non-numeric/NaN/null metric'})
            for name in expected:
                if metric(row['metrics'], name) is None:
                    csv_subtask({'wall_time_ms': s['wall_time_ms'], 'phase': s['phase'], 'operator': row['operator'],
                                 'parallelism': row['parallelism'], 'subtask': row['subtask'], 'taskmanager_id': row['taskmanager_id'],
                                 'metric': name, 'value': None, 'status': 'Missing', 'missing_reason': 'not registered or ambiguous'})
        return s
    try:
        command(['docker', 'compose', 'up', '-d', '--wait', '--wait-timeout', '180', 'kafka', 'jobmanager', 'taskmanager'], 240)
        active = [j for j in rest(base, '/jobs/overview')['jobs'] if j['state'] not in ['CANCELED', 'FAILED', 'FINISHED']]
        if active:
            raise RuntimeError('Other active jobs exist; exclusive resource budget required')
        wait_for(lambda: rest(base, '/overview')['slots-available'] >= max(cfg['operator_parallelism'].values()), 'free slots', 60)
        metadata['environment'] = environment(base)
        metadata['flink_configuration'] = rest(base, '/jobmanager/config')
        clocks = metadata['environment']['clock_probes'].copy()
        events({'state': 'PREFLIGHT', 'wall_time_ms': time.time_ns() // 1000000})
        preflight_result = preflight(cfg)
        metadata['preflight'] = preflight_result
        if preflight_result.get('interrupted'):
            raise KeyboardInterrupt('Preflight interrupted')
        if preflight_result.get('error') or preflight_result.get('cleanup_errors'):
            raise RuntimeError('Preflight execution/cleanup failed: '+str(preflight_result.get('error') or preflight_result['cleanup_errors']))
        if not preflight_result['valid']:
            events({'state': 'PREFLIGHT_REJECTED', 'reasons': preflight_result['reasons']})
        else:
            create_topic(topic, cfg['kafka_partitions']); created = True
            expected = cfg['kafka_partitions'] + math.floor(cfg['target_rate'] * cfg['warmup_seconds']) + math.floor(cfg['target_rate'] * cfg['measurement_seconds'])
            verification = expected if mode == 'smoke' and expected <= 10000 else 0
            metadata['verification_records'] = verification
            args = job_args(cfg, topic, group, run_id, verification, experiment_id)
            metadata['job_args'] = args
            job_id = submit(base, args); metadata['job_id'] = job_id
            metadata['actual_job_graph'] = wait_running(base, job_id)
            collector = FlinkCollector(base, job_id, cfg)
            relative = store.directory.relative_to(ROOT / 'results')
            command(['docker', 'compose', 'run', '--rm', '--no-deps', '--name', probe_name, 'producer',
                     '--topic', topic, '--experiment-id', experiment_id, '--run-id', run_id, '--generation-phase', 'warmup',
                     '--total-records', str(cfg['kafka_partitions']), '--target-rate', str(cfg['target_rate']),
                     '--payload-size', str(cfg['payload_size']), '--number-of-users', str(cfg['number_of_users']),
                     '--distribution', cfg['distribution'], '--hot-key-ratio', str(cfg['hot_key_ratio']), '--seed', str(cfg['seed']),
                     '--kafka-partitions', str(cfg['kafka_partitions']), '--output', '/results/' + str(relative / 'readiness-producer.json')], 90)
            def ready():
                s = collect(collector)
                return s if s['counts']['sink_completed'] == cfg['kafka_partitions'] else None
            wait_for(ready, 'real source-to-sink readiness data', 60)
            events({'state': 'SOURCE_READY', 'wall_time_ms': time.time_ns() // 1000000})
            log = (store.directory / 'producer.log').open('x')
            process = subprocess.Popen(['docker', 'compose', 'run', '--rm', '--no-deps', '--name', producer_name,
                                        '--entrypoint', 'python', 'producer', '/app/session.py',
                                        *session_args(cfg, store.directory, topic, group, run_id, cfg['warmup_seconds'],
                                                      cfg['measurement_seconds'], cfg['kafka_partitions'], experiment_id)], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
            def planned():
                lines = read_jsonl(store.directory / 'producer.jsonl')
                if process.poll() is not None and not lines:
                    raise RuntimeError('Producer failed before plan; see producer.log')
                return lines[0]['plan'] if lines else None
            plan = wait_for(planned, 'producer phase plan', 45)
            metadata['plan'] = plan
            metadata['producer_container_inspect'] = json.loads(command(['docker', 'inspect', producer_name]))
            clocks['producer'] = producer_clock(producer_name)
            metadata['producer_clock_probe'] = clocks['producer']
            deadline = plan['measurement_end_wall_ms'] / 1000 + cfg['drain_timeout_seconds']
            next_sample = time.monotonic(); previous_phase = None
            while time.time() <= deadline:
                s = collect(collector); final = s
                if s['phase'] != previous_phase:
                    events({'state': s['phase'].upper(), 'wall_time_ms': s['wall_time_ms']}); previous_phase = s['phase']
                if process.poll() is not None:
                    if process.returncode:
                        raise RuntimeError('Producer execution failed; inspect producer.json/log/telemetry')
                    producer = json.loads((store.directory / 'producer.json').read_text())
                    expected_actual = cfg['kafka_partitions'] + sum(v.get('sent', 0) for v in producer['counts'].values())
                    if s['counts']['sink_completed'] is not None and s['counts']['sink_completed'] > expected_actual:
                        audit = {'error': 'Sink completed more records than producer sent'}
                        break
                    if s['counts']['sink_completed'] == expected_actual and s['counts']['source_records'] == expected_actual and s['offsets']['read_backlog'] == 0 and s['checkpoints']['counts']['completed'] > 0:
                        if verification:
                            try:
                                audit = audit_sink(s['subtasks'], expected_actual)
                            except RuntimeError as exc:
                                audit = {'error': str(exc)}
                        break
                next_sample += cfg['sample_interval_seconds']
                delay = max(0, next_sample - time.monotonic())
                if delay:
                    time.sleep(min(delay, cfg['sample_interval_seconds']))
            if producer is None and process.poll() is None:
                raise TimeoutError('Producer did not finish within drain deadline')
            if final and producer and final['counts']['sink_completed'] != cfg['kafka_partitions'] + sum(v.get('sent', 0) for v in producer['counts'].values()):
                events({'state': 'DRAIN_TIMEOUT', 'wall_time_ms': time.time_ns() // 1000000})
            metadata['drain_timeout'] = bool(final and producer and final['counts']['measurement_cohort_completed'] != producer['counts']['measurement'].get('sent', 0))
    except (Exception, KeyboardInterrupt) as exc:
        errors.append(repr(exc)); events({'state': 'ERROR', 'error': repr(exc), 'wall_time_ms': time.time_ns() // 1000000})
    finally:
        if process is not None and process.poll() is None:
            try:
                command(['docker', 'stop', '--time', '10', producer_name], 20)
            except Exception as exc:
                errors.append('producer stop: ' + repr(exc))
        # Recover actual Job ID from unique name if submission failed after server acceptance.
        if job_id is None and created:
            try:
                matching = [j['jid'] for j in rest(base, '/jobs/overview')['jobs'] if j['name'] == 'parallelism-benchmark/' + experiment_id + '/' + run_id]
                job_id = matching[0] if len(matching) == 1 else None
            except Exception as exc:
                errors.append('job discovery: ' + repr(exc))
        if job_id:
            try:
                store.json('job-exceptions.json', rest(base, '/jobs/' + job_id + '/exceptions'))
            except Exception as exc:
                errors.append('exception collection: ' + repr(exc))
        cleanup, cleanup_errors = cleanup_run(base, job_id, producer_name, topic, created)
        cleanup_errors += ['preflight: '+str(e) for e in preflight_result.get('cleanup_errors',[])]
        _, probe_errors = cleanup_run(base, None, probe_name, topic, False)
        _, observer_errors = cleanup_run(base, None, observer_name, topic, False)
        errors.extend(cleanup_errors + probe_errors + observer_errors)
        metadata['cleanup'] = cleanup; metadata['cleanup_errors'] = cleanup_errors + probe_errors + observer_errors
        if process is not None:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate(); process.wait(timeout=10)
        if log:
            log.close()
        if producer is None and (store.directory / 'producer.json').exists():
            try:
                producer = json.loads((store.directory / 'producer.json').read_text())
            except ValueError:
                pass
        try:
            telemetry = read_jsonl(store.directory / 'producer.jsonl')
            obs = observations(samples, final, producer, telemetry, preflight_result, cfg, mode, clocks, audit, errors)
        except Exception as exc:
            errors.append('final observations: ' + repr(exc))
            obs = {'execution_errors': errors, 'producer_valid': False}
        if not preflight_result['valid'] and not errors:
            obs = {'producer_valid': False, 'preflight_valid': False, 'preflight': preflight_result}
        summary = classify(obs, cfg['thresholds'], mode)
        summary.update(run_id=run_id, experiment_id=experiment_id, run_context=context, mode=mode, job_id=job_id, result_directory=str(store.directory), drain_timeout=metadata.get('drain_timeout'),
                       final_job_state=cleanup.get('final_job_state'), cleanup_errors=cleanup_errors + probe_errors + observer_errors)
        metadata['execution_validity'] = summary['execution_validity']; metadata['performance_stability'] = summary['performance_stability']
        store.json('metadata.json', metadata); store.json('summary.json', summary)
        events({'state': 'FINALIZED', 'execution_validity': summary['execution_validity'], 'performance_stability': summary['performance_stability']})
        store.close()
    print(json.dumps({'result_directory': str(store.directory), 'execution_validity': summary['execution_validity'],
                      'performance_stability': summary['performance_stability'], 'reasons': summary['reasons']}, indent=2, ensure_ascii=False), flush=True)
    return summary


def numeric(value):
    from lab import numeric_metric
    return numeric_metric(value)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['smoke', 'full'], default='smoke')
    parser.add_argument('--config'); parser.add_argument('--rest-url', default='http://localhost:8081')
    parser.add_argument('--target-rate', type=float); parser.add_argument('--distribution', choices=['uniform', 'skewed'])
    parser.add_argument('--seed', type=int); parser.add_argument('--warmup', type=float); parser.add_argument('--measurement', type=float)
    parser.add_argument('--drain-timeout', type=float); parser.add_argument('--output-dir', type=Path, default=ROOT / 'results' / 'phase2')
    a = parser.parse_args()
    directory = a.output_dir.resolve()
    if not directory.is_relative_to(ROOT / 'results'):
        parser.error('output-dir must be below results/ (Compose mount)')
    cfg = load_config(a.config, a.mode, {'target_rate': a.target_rate, 'distribution': a.distribution, 'seed': a.seed,
                      'warmup_seconds': a.warmup, 'measurement_seconds': a.measurement, 'drain_timeout_seconds': a.drain_timeout})
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt('SIGTERM')))
    result = run(cfg, a.mode, a.rest_url, directory)
    raise SystemExit(1 if result['execution_validity'] in ['ERROR', 'INVALID'] else 0)
