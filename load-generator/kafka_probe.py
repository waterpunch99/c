"""Admin API next-offset snapshots; never substitute committed offsets for source reads."""
import time


def offsets(admin, topic, group, partitions):
    from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
    from confluent_kafka.admin import OffsetSpec
    started = time.time_ns() // 1000000
    request = {TopicPartition(topic, i): OffsetSpec.latest() for i in range(partitions)}
    futures = admin.list_offsets(request, request_timeout=5)
    rows = []
    for tp, future in futures.items():
        offset = future.result(timeout=6).offset
        rows.append({"partition": tp.partition, "log_end_offset": offset if offset >= 0 else None,
                     "committed_offset": None, "committed_status": "Missing"})
    error = None
    try:
        future = admin.list_consumer_group_offsets([
            ConsumerGroupTopicPartitions(group, [TopicPartition(topic, i) for i in range(partitions)])], request_timeout=5)[group]
        committed = {tp.partition: tp.offset for tp in future.result(timeout=6).topic_partitions}
        for row in rows:
            value = committed.get(row['partition'])
            row['committed_offset'] = value if value is not None and value >= 0 else None
            row['committed_status'] = 'Available' if row['committed_offset'] is not None else 'Missing'
    except Exception as exc:
        error = repr(exc)  # groups without checkpoints/commits can legitimately have no offsets
    return {"started_wall_ms": started, "wall_time_ms": time.time_ns() // 1000000,
            "partitions": sorted(rows, key=lambda r: r['partition']), "committed_missing_reason": error,
            "error": None}


if __name__ == '__main__':
    import argparse
    import json
    from confluent_kafka.admin import AdminClient
    p = argparse.ArgumentParser()
    p.add_argument('--topic', required=True); p.add_argument('--group', required=True)
    p.add_argument('--partitions', type=int, required=True)
    a = p.parse_args()
    print(json.dumps(offsets(AdminClient({'bootstrap.servers': 'kafka:29092'}), a.topic, a.group, a.partitions)))
