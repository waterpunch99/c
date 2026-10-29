"""Phase 1 minimal monotonic open-loop producer. Not a capacity preflight tool."""
import argparse
import json
import random
import re
import time
from collections import Counter
from pathlib import Path


def make_event(sequence, rng, args):
    hot = args.distribution == "skewed" and rng.random() < args.hot_key_ratio
    user = 1 if hot else rng.randint(2 if args.distribution == "skewed" else 1, args.number_of_users)
    return {
        "eventId": f"{args.run_id}:{sequence}", "sequence": sequence,
        "userId": user, "producerTimestamp": time.time_ns() // 1_000_000,
        "payload": "x" * args.payload_size, "experimentId": args.experiment_id,
        "runId": args.run_id, "generationPhase": args.generation_phase,
    }


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap-servers", default="kafka:29092")
    p.add_argument("--topic", required=True)
    p.add_argument("--target-rate", type=float, default=200)
    p.add_argument("--duration", type=float, default=10)
    p.add_argument("--total-records", type=int)
    p.add_argument("--distribution", choices=["uniform", "skewed"], default="uniform")
    p.add_argument("--number-of-users", type=int, default=1_000_000)
    p.add_argument("--payload-size", type=int, default=256)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--hot-key-ratio", type=float, default=0.4)
    p.add_argument("--kafka-partitions", type=int, default=32)
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--generation-phase", choices=["warmup", "measurement"], default="measurement")
    p.add_argument("--output", required=True)
    a = p.parse_args(argv)
    import math
    if not math.isfinite(a.target_rate) or not math.isfinite(a.duration) or a.target_rate <= 0 or a.duration <= 0:
        p.error("rate and duration must be finite and positive")
    if a.number_of_users < 2 or a.payload_size < 0 or a.kafka_partitions < 1 or not 0 <= a.hot_key_ratio <= 1:
        p.error("invalid users/payload/partitions/hot-key-ratio")
    if a.total_records is not None and a.total_records < 1:
        p.error("total-records must be positive")
    if a.total_records is None and int(a.target_rate * a.duration) < 1:
        p.error("duration and rate must schedule at least one record")
    if not all(re.fullmatch(r"[A-Za-z0-9_-]+", v) for v in [a.experiment_id, a.run_id]):
        p.error("identifiers must contain letters, digits, _, -")
    return a


def run(args):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Reserve result before producing anything. Existing runs are never overwritten.
    with out.open("x", encoding="utf-8") as result_file:
        producer = Producer({"bootstrap.servers": args.bootstrap_servers, "acks": "all",
                             "enable.idempotence": True, "linger.ms": 5, "delivery.timeout.ms": 30000})
        metadata = AdminClient({"bootstrap.servers": args.bootstrap_servers}).list_topics(args.topic, timeout=15)
        topic = metadata.topics.get(args.topic)
        if topic is None or topic.error or len(topic.partitions) != args.kafka_partitions:
            raise RuntimeError("Topic missing or partition count mismatch")
        rng = random.Random(args.seed)
        scheduled = sent = acked = errors = queue_full = hot_count = 0
        partitions = Counter()
        ack_partitions = Counter()
        max_lateness = 0.0
        start = time.monotonic()
        count = args.total_records if args.total_records is not None else int(args.target_rate * args.duration)
        interrupted = None

        def delivered(err, msg):
            nonlocal acked, errors
            if err is not None:
                errors += 1
            else:
                acked += 1
                ack_partitions[msg.partition()] += 1

        try:
            for seq in range(count):
                due = start + seq / args.target_rate
                delay = due - time.monotonic()
                if delay > 0:
                    # poll callbacks while waiting, independent of Flink progress
                    producer.poll(delay)
                    remaining = due - time.monotonic()
                    if remaining > 0:
                        time.sleep(remaining)
                scheduled += 1
                max_lateness = max(max_lateness, time.monotonic() - due)
                event = make_event(seq, rng, args)
                hot_count += event["userId"] == 1
                partition = seq % args.kafka_partitions  # independent of userId
                value = json.dumps(event, separators=(",", ":")).encode()
                try:
                    producer.produce(args.topic, value=value, partition=partition, on_delivery=delivered)
                    sent += 1
                    partitions[partition] += 1
                except BufferError:
                    # Failed schedule slots are recorded, not hidden by backoff/retry pacing.
                    queue_full += 1
                producer.poll(0)
        except (KeyboardInterrupt, Exception) as exc:
            interrupted = repr(exc)
        finally:
            send_end = time.monotonic()
            pending = producer.flush(35)
            end = time.monotonic()
            send_seconds = send_end - start
            # Sparse finite batches have n-1 intervals; report both schedule-span and ACK-inclusive rates.
            interval_rate = (sent - 1) / send_seconds if sent > 1 and send_seconds else None
            summary = {
                "kind": "phase1_producer_observation", "config": vars(args),
                "target_rate": args.target_rate, "scheduled_records": scheduled, "sent_records": sent,
                "acknowledged_records": acked, "delivery_errors": errors, "queue_full_errors": queue_full,
                "pending_after_flush": pending, "send_seconds": send_seconds, "ack_seconds": end - start,
                "actual_sent_interval_rate": interval_rate, "ack_inclusive_rate": acked / (end - start),
                "max_schedule_lateness_seconds": max_lateness,
                "generated_hot_key_ratio": hot_count / scheduled if scheduled else None,
                "sent_partition_counts": dict(partitions), "acknowledged_partition_counts": dict(ack_partitions),
                "interrupted": interrupted, "producer_retries": None,
                "missing_metrics": {"producer_retries": "Phase 2 producer statistics collector not implemented"},
            }
            json.dump(summary, result_file, ensure_ascii=False, indent=2)
            result_file.write("\n")
        if interrupted or pending or errors or queue_full or acked != count:
            raise RuntimeError("Producer incomplete; inspect saved observations")
        print(json.dumps({"sent": sent, "acknowledged": acked, "output": str(out)}))
        return summary


if __name__ == "__main__":
    run(parse_args())
