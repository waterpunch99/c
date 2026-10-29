"""Finite monotonic open-loop load with cohort/window accounting and append-only telemetry."""
import argparse
import json
import math
import random
import signal
import threading
import time
from collections import Counter
from pathlib import Path
from producer import make_event
from resource_probe import probe
from kafka_probe import offsets


def histogram_add(hist, value):
    hist[min(63, max(0, int(value)).bit_length())] += 1


def run(a):
    import sys
    import confluent_kafka
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    summary_path = Path(a.output)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()
    finished = threading.Event()
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, lambda *_: stop.set())
    # Reserve all output files before sending records.
    with summary_path.open('x') as summary_file, Path(a.telemetry_output).open('x') as telemetry, Path(a.kafka_output).open('x') as kafka_file:
        admin = AdminClient({'bootstrap.servers': a.bootstrap_servers})
        meta = admin.list_topics(a.topic, timeout=10).topics.get(a.topic)
        if meta is None or meta.error or len(meta.partitions) != a.kafka_partitions:
            raise RuntimeError('Topic metadata/partition mismatch')
        counts = {phase: Counter() for phase in ['warmup', 'measurement']}
        part_sent = {phase: Counter() for phase in counts}
        part_acked = {phase: Counter() for phase in counts}
        latencies = {phase: [0] * 64 for phase in counts}
        send_times = {phase: [0] * 64 for phase in counts}
        latest_stats = {}
        max_lateness = {phase: 0.0 for phase in counts}
        start_wall = (math.floor(time.time()) + 2) * 1000
        measure_start = start_wall + int(a.warmup_duration * 1000)
        measure_end = measure_start + int(a.duration * 1000)
        start_mono = time.monotonic() + (start_wall / 1000 - time.time())
        plan = {'start_wall_ms': start_wall, 'measurement_start_wall_ms': measure_start,
                'measurement_end_wall_ms': measure_end, 'measurement_seconds': a.duration,
                'warmup_seconds': a.warmup_duration, 'target_rate': a.target_rate}
        interrupted = None
        previous = {phase: Counter() for phase in counts}
        previous_mono = time.monotonic()
        def emit(kind='sample'):
            nonlocal previous, previous_mono
            now = time.monotonic()
            elapsed = now - previous_mono
            totals = {phase: dict(values) for phase, values in counts.items()}
            deltas = {phase: {key: values[key] - previous[phase][key] for key in
                             ['scheduled', 'generated', 'sent', 'acked', 'errors', 'queue_full', 'expired']}
                      for phase, values in counts.items()}
            row = {'kind': kind, 'wall_time_ms': time.time_ns() // 1000000, 'monotonic': now,
                   'interval_seconds': elapsed, 'plan': plan, 'counts': totals, 'deltas': deltas,
                   'rates': {phase: {key: value / elapsed if elapsed else None for key, value in delta.items()}
                             for phase, delta in deltas.items()}, 'producer_queue_messages': len(p),
                   'producer_stats': latest_stats.copy(), 'resource': probe(), 'max_lateness_seconds': max_lateness.copy()}
            telemetry.write(json.dumps(row) + '\n'); telemetry.flush()
            previous = {phase: values.copy() for phase, values in counts.items()}; previous_mono = now

        def stats_cb(raw):
            stats = json.loads(raw)
            latest_stats.clear()
            latest_stats.update({k: stats.get(k) for k in ['msg_cnt', 'msg_size', 'msg_max', 'msg_size_max', 'txmsgs', 'txmsg_bytes']})
            brokers = list(stats.get('brokers', {}).values())
            latest_stats['retries'] = sum(b['txretries'] for b in brokers) if brokers and all('txretries' in b for b in brokers) else None
            latest_stats['brokers'] = {k: {field: b.get(field) for field in ['outbuf_cnt', 'waitresp_cnt', 'rtt', 'throttle']}
                                      for k, b in stats.get('brokers', {}).items()}

        p = Producer({'bootstrap.servers': a.bootstrap_servers, 'acks': 'all', 'enable.idempotence': True,
                      'linger.ms': 5, 'delivery.timeout.ms': 30000, 'statistics.interval.ms': 1000, 'stats_cb': stats_cb})
        def observe_kafka():
            while not finished.is_set():
                try:
                    row = offsets(admin, a.topic, a.group_id, a.kafka_partitions)
                except Exception as exc:
                    row = {'wall_time_ms': time.time_ns() // 1000000, 'partitions': None, 'error': repr(exc)}
                kafka_file.write(json.dumps(row) + '\n'); kafka_file.flush()
                finished.wait(a.sample_interval)
        observer = threading.Thread(target=observe_kafka, daemon=True)
        observer.start()
        emit('plan')
        rng = random.Random(a.seed)
        seq = a.sequence_start
        next_emit = time.monotonic() + a.sample_interval
        try:
            for phase, duration, phase_start in [('warmup', a.warmup_duration, start_mono),
                                                  ('measurement', a.duration, start_mono + a.warmup_duration)]:
                a.generation_phase = phase
                planned_count = math.floor(a.target_rate * duration)
                for index in range(planned_count):
                    if stop.is_set():
                        interrupted = 'signal'; break
                    due = phase_start + index / a.target_rate
                    while time.monotonic() < due and not stop.is_set():
                        p.poll(max(0, min(0.05, due - time.monotonic())))
                        if time.monotonic() >= next_emit:
                            emit(); next_emit = time.monotonic() + a.sample_interval
                    if stop.is_set():
                        interrupted = 'signal'; break
                    counts[phase]['scheduled'] += 1
                    late = max(0, time.monotonic() - due)
                    max_lateness[phase] = max(max_lateness[phase], late)
                    if time.monotonic() >= phase_start + duration:
                        counts[phase]['expired'] += 1
                        continue
                    event = make_event(seq, rng, a)
                    event.update(measurementStartTimestamp=measure_start, measurementEndTimestamp=measure_end)
                    counts[phase]['generated'] += 1
                    counts[phase]['hot'] += event['userId'] == 1
                    partition = seq % a.kafka_partitions if a.partition_strategy == 'round-robin' else rng.randrange(a.kafka_partitions)
                    value = json.dumps(event, separators=(',', ':')).encode()
                    sent_at = time.monotonic()
                    def delivered(err, msg, phase=phase, sent_at=sent_at, is_hot=event['userId'] == 1):
                        if err is not None:
                            counts[phase]['errors'] += 1
                        else:
                            counts[phase]['acked'] += 1
                            counts[phase]['acked_hot'] += is_hot
                            part_acked[phase][msg.partition()] += 1
                            now_ms = time.time_ns() // 1000000
                            if measure_start <= now_ms < measure_end:
                                counts[phase]['acked_in_measurement_window'] += 1
                            histogram_add(latencies[phase], (time.monotonic() - sent_at) * 1e6)
                    try:
                        p.produce(a.topic, value=value, partition=partition, on_delivery=delivered)
                        counts[phase]['sent'] += 1; part_sent[phase][partition] += 1
                        counts[phase]['sent_hot'] += event['userId'] == 1
                        seq += 1
                    except BufferError:
                        counts[phase]['queue_full'] += 1
                    histogram_add(send_times[phase], (time.monotonic() - sent_at) * 1e6)
                    p.poll(0)
                    if time.monotonic() >= next_emit:
                        emit(); next_emit = time.monotonic() + a.sample_interval
                if interrupted:
                    break
            # Finish the complete fixed duration, even when the last schedule slot is earlier.
            while not stop.is_set() and time.monotonic() < start_mono + a.warmup_duration + a.duration:
                p.poll(max(0, min(0.05, start_mono + a.warmup_duration + a.duration - time.monotonic())))
                if time.monotonic() >= next_emit:
                    emit(); next_emit = time.monotonic() + a.sample_interval
        except Exception as exc:
            interrupted = repr(exc)
        finally:
            send_end_wall = time.time_ns() // 1000000
            emit('send_end')
            pending = p.flush(35)
            emit('final')
            finished.set(); observer.join(timeout=15)
            final_kafka = offsets(admin, a.topic, a.group_id, a.kafka_partitions)
            kafka_file.write(json.dumps(final_kafka) + '\n'); kafka_file.flush()
            summary = {'kind': 'phase2_producer_observation', 'plan': plan, 'config': vars(a),
                       'runtime': {'python': sys.version, 'confluent_kafka': confluent_kafka.version(), 'librdkafka': confluent_kafka.libversion()},
                       'counts': {phase: dict(values) for phase, values in counts.items()},
                       'sent_partition_counts': {phase: dict(v) for phase, v in part_sent.items()},
                       'acknowledged_partition_counts': {phase: dict(v) for phase, v in part_acked.items()},
                       'ack_latency_us_log2_buckets': latencies, 'produce_call_us_log2_buckets': send_times,
                       'producer_stats': latest_stats, 'pending_after_flush': pending, 'interrupted': interrupted,
                       'send_end_wall_ms': send_end_wall, 'max_lateness_seconds': max_lateness,
                       'sent_rate': counts['measurement']['sent'] / a.duration,
                       'acknowledged_cohort_rate': counts['measurement']['acked'] / a.duration,
                       'acknowledged_window_rate': sum(v['acked_in_measurement_window'] for v in counts.values()) / a.duration,
                       'actual_hot_key_ratio': counts['measurement']['hot'] / counts['measurement']['generated'] if counts['measurement']['generated'] else None}
            json.dump(summary, summary_file, indent=2); summary_file.write('\n')
        if interrupted or pending or any(v['errors'] for v in counts.values()):
            raise RuntimeError('Producer interrupted or incomplete; raw telemetry preserved')


def parse_args(argv=None):
    from producer import parse_args as basic_args
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--warmup-duration', type=float, default=0)
    parser.add_argument('--sample-interval', type=float, default=5)
    parser.add_argument('--sequence-start', type=int, default=0)
    parser.add_argument('--telemetry-output', required=True)
    parser.add_argument('--kafka-output', required=True)
    parser.add_argument('--group-id', required=True)
    parser.add_argument('--partition-strategy', choices=['round-robin', 'random'], default='round-robin')
    parser.add_argument('--producer-workers', type=int, choices=[1], default=1)
    session_args, extra = parser.parse_known_args(argv)
    args = basic_args(extra)
    if args.total_records is not None:
        parser.error('session uses fixed durations; total-records is not supported')
    for name, value in vars(session_args).items():
        setattr(args, name, value)
    if args.sequence_start < 0 or not math.isfinite(args.warmup_duration) or args.warmup_duration < 0 or not math.isfinite(args.sample_interval) or args.sample_interval <= 0:
        parser.error('invalid sequence/warmup/sample interval')
    return args


if __name__ == '__main__':
    run(parse_args())
