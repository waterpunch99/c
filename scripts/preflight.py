"""Producer/Kafka validation on an isolated topic, before any capacity run."""
import argparse
import json
import uuid
from lab import ROOT, command
from phase2_config import load_config
from measurement import producer_decision
from result_store import RunStore, read_jsonl
from smoke_test import cleanup_run


def create_topic(topic, partitions):
    return command(['docker', 'compose', 'exec', '-T', 'kafka', '/opt/kafka/bin/kafka-topics.sh',
                    '--bootstrap-server', 'kafka:29092', '--create', '--topic', topic,
                    '--partitions', str(partitions), '--replication-factor', '1'])


def session_args(cfg, directory, topic, group, run_id, warmup, duration, sequence=0, experiment_id='phase2'):
    relative = directory.relative_to(ROOT / 'results')
    path = '/results/' + str(relative)
    return ['--topic', topic, '--group-id', group, '--experiment-id', experiment_id, '--run-id', run_id,
            '--target-rate', str(cfg['target_rate']), '--warmup-duration', str(warmup), '--duration', str(duration),
            '--kafka-partitions', str(cfg['kafka_partitions']), '--distribution', cfg['distribution'],
            '--number-of-users', str(cfg['number_of_users']), '--payload-size', str(cfg['payload_size']),
            '--seed', str(cfg['seed']), '--hot-key-ratio', str(cfg['hot_key_ratio']), '--sequence-start', str(sequence),
            '--partition-strategy', cfg['partition_strategy'], '--sample-interval', str(cfg['sample_interval_seconds']),
            '--output', path + '/producer.json', '--telemetry-output', path + '/producer.jsonl', '--kafka-output', path + '/kafka.jsonl']


def preflight(cfg, output_dir=None):
    run_id = 'preflight_' + uuid.uuid4().hex
    directory = (output_dir or ROOT / 'results' / 'preflight') / run_id
    store = RunStore(directory)
    topic = 'lab-' + run_id.replace('_', '-')
    group = 'group-' + run_id
    name = 'producer-' + run_id
    created = False
    result = {'valid': False, 'reasons': [], 'directory': str(directory), 'run_id': run_id}
    store.json('metadata.json', {'config': cfg, 'topic': topic, 'group_id': group, 'kind': 'producer_kafka_preflight'})
    try:
        create_topic(topic, cfg['kafka_partitions']); created = True
        output = command(['docker', 'compose', 'run', '--rm', '--no-deps', '--name', name,
                          '--entrypoint', 'python', 'producer', '/app/session.py',
                          *session_args(cfg, directory, topic, group, run_id, 0, cfg['preflight_seconds'])],
                         timeout=cfg['preflight_seconds'] + 70)
        (directory / 'producer.log').write_text(output)
        producer = json.loads((directory / 'producer.json').read_text())
        result.update(producer_decision(producer, read_jsonl(directory / 'producer.jsonl'), cfg))
        final_kafka = read_jsonl(directory / 'kafka.jsonl')[-1]
        leo = sum(r['log_end_offset'] for r in final_kafka['partitions'])
        if leo != producer['counts']['measurement'].get('acked', 0):
            result['valid'] = False; result['reasons'].append('preflight LEO total does not equal Kafka ACK count')
        result['kafka_log_end_total'] = leo
        result['producer_resource_contention_warning'] = 'Producer, Kafka and Flink share host CPU; preflight is necessary but not sufficient'
    except (Exception, KeyboardInterrupt) as exc:
        result['valid'] = False; result['error'] = repr(exc)
        result['interrupted'] = isinstance(exc, KeyboardInterrupt)
        result['reasons'].append('preflight execution failed')
    finally:
        cleanup, errors = cleanup_run('http://localhost:8081', None, name, topic, created)
        result['cleanup'] = cleanup; result['cleanup_errors'] = errors
        if errors:
            result['valid'] = False
            result['reasons'].append('preflight resource cleanup failed')
        store.json('summary.json', result); store.close()
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config'); parser.add_argument('--target-rate', type=float)
    parser.add_argument('--duration', type=float, default=10)
    a = parser.parse_args()
    cfg = load_config(a.config, overrides={'target_rate': a.target_rate, 'preflight_seconds': a.duration})
    command(['docker', 'compose', 'up', '-d', '--wait', '--wait-timeout', '180', 'kafka'], timeout=240)
    result = preflight(cfg); print(json.dumps(result, indent=2, ensure_ascii=False))
    raise SystemExit(0 if result['valid'] else 1)
