"""Unit fixtures only. These tests never write benchmark measurements to results/."""
import importlib.util
import json
import random
import sys
import tempfile
import types
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("producer", ROOT / "load-generator" / "producer.py")
producer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(producer)
from lab import audit_sink, find_metric, numeric_metric, save_json, metric_query_paths, snapshot_subtasks
import smoke_test


def args(*extra):
    return producer.parse_args(["--topic", "fixture", "--experiment-id", "test", "--run-id", "test", "--output", "fixture.json", *extra])


class DistributionTest(unittest.TestCase):
    def test_uniform_and_reproducible(self):
        a = args("--number-of-users", "100", "--payload-size", "8")
        rng = random.Random(a.seed)
        users = Counter(producer.make_event(i, rng, a)["userId"] for i in range(100000))
        self.assertEqual(len(users), 100)
        self.assertLess(max(abs(c - 1000) for c in users.values()), 150)
        self.assertEqual(producer.make_event(0, random.Random(42), a)["userId"],
                         producer.make_event(0, random.Random(42), a)["userId"])
        self.assertEqual(len(producer.make_event(0, random.Random(42), a)["payload"]), 8)

    def test_skew_and_partition_independence(self):
        a = args("--distribution", "skewed")
        rng = random.Random(a.seed)
        users = Counter(); partitions = Counter(); hot_partitions = Counter()
        for i in range(100000):
            e = producer.make_event(i, rng, a)
            users[e["userId"]] += 1
            partitions[i % a.kafka_partitions] += 1
            if e["userId"] == 1:
                hot_partitions[i % a.kafka_partitions] += 1
        self.assertAlmostEqual(users[1] / 100000, 0.4, delta=0.006)
        self.assertLessEqual(max(partitions.values()) - min(partitions.values()), 1)
        self.assertEqual(len(hot_partitions), a.kafka_partitions)

    def test_invalid_args(self):
        for pair in [("--target-rate", "0"), ("--target-rate", "nan"), ("--total-records", "0"),
                     ("--kafka-partitions", "0"), ("--hot-key-ratio", "1.1")]:
            with self.subTest(pair=pair), self.assertRaises(SystemExit), patch("sys.stderr"):
                args(*pair)


class MetricAuditTest(unittest.TestCase):
    def row(self, count, mask, seqsum=3):
        return {"operator": "Sink: Measurement Sink", "metrics": {
            "Measurement Sink.completedRecords": str(count), "Measurement Sink.duplicateRecords": "0",
            "Measurement Sink.sequenceSum": str(seqsum), "Measurement Sink.verificationSeenWord0": str(mask)}}

    def test_missing_not_zero_and_large_int_exact(self):
        for value in [None, "NaN", "unsupported", "Infinity"]:
            self.assertIsNone(numeric_metric(value))
        self.assertEqual(numeric_metric("9223372036854775807"), 9223372036854775807)
        self.assertEqual(numeric_metric("0"), 0)
        with self.assertRaises(RuntimeError):
            find_metric({}, "completedRecords")

    def test_full_audit_and_incomplete(self):
        self.assertEqual(audit_sink([self.row(3, 7)], 3)["unique_event_ids"], 3)
        self.assertIsNone(audit_sink([self.row(2, 3, 1)], 3))

    def test_duplicates_across_subtasks_and_missing_id(self):
        with self.assertRaises(RuntimeError):
            audit_sink([self.row(1, 1, 0), self.row(2, 3, 1)], 3)
        with self.assertRaises(RuntimeError):
            audit_sink([self.row(3, 3)], 3)

    def test_no_result_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "result.json"
            save_json(path, {"fixture": 1})
            with self.assertRaises(FileExistsError):
                save_json(path, {"fixture": 2})
            self.assertEqual(json.loads(path.read_text()), {"fixture": 1})

    def test_long_source_metric_ids_respect_rest_request_limit(self):
        import urllib.parse
        path = "/jobs/" + "a" * 32 + "/vertices/" + "b" * 32 + "/subtasks/0/metrics"
        ids = [f"Source__Kafka_Source.KafkaSourceReader.topic.a-long-smoke-topic.partition.{i}.currentOffset" for i in range(200)]
        queries = list(metric_query_paths(path, ids))
        self.assertGreater(len(queries), 1)
        recovered = []
        for query in queries:
            self.assertLessEqual(len(query), 3000)
            recovered.extend(urllib.parse.parse_qs(urllib.parse.urlsplit(query).query)["get"][0].split(","))
        self.assertEqual(recovered, ids)

    def test_newly_registered_metrics_are_discovered_after_initial_snapshot(self):
        inventory = iter([[], [{"id": "Parse.parseErrors"}]])
        def response(base, path):
            if path.endswith("/vertices/v"):
                return {"subtasks": [{"subtask": 0, "status": "RUNNING", "taskmanager-id": "tm"}]}
            if "?get=" in path:
                return [{"id": "Parse.parseErrors", "value": "0"}]
            return next(inventory)
        vertices = [{"id": "v", "name": "Parse", "parallelism": 1}]
        with patch("lab.rest", side_effect=response):
            first, available = snapshot_subtasks("http://fixture", "job", vertices)
            self.assertEqual(first[0]["metrics"], {})
            second, _ = snapshot_subtasks("http://fixture", "job", vertices, available)
            self.assertEqual(find_metric(second[0]["metrics"], "parseErrors"), 0)


class ProducerTest(unittest.TestCase):
    def test_schedule_ack_and_partition_counts_with_fake_transport(self):
        clock = types.SimpleNamespace(now=100.0)
        def poll(delay):
            clock.now += delay
        class FakeProducer:
            def __init__(self, config): pass
            def poll(self, delay): poll(delay)
            def produce(self, topic, value, partition, on_delivery):
                on_delivery(None, types.SimpleNamespace(partition=lambda: partition))
            def flush(self, timeout): return 0
        class FakeAdmin:
            def __init__(self, config): pass
            def list_topics(self, topic, timeout):
                return types.SimpleNamespace(topics={topic: types.SimpleNamespace(error=None, partitions=list(range(32)))})
        kafka = types.ModuleType("confluent_kafka"); kafka.Producer = FakeProducer
        admin = types.ModuleType("confluent_kafka.admin"); admin.AdminClient = FakeAdmin
        with tempfile.TemporaryDirectory() as d, patch.dict(sys.modules, {"confluent_kafka": kafka, "confluent_kafka.admin": admin}), \
                patch.object(producer.time, "monotonic", lambda: clock.now), patch.object(producer.time, "sleep", poll), patch("builtins.print"):
            a = args("--total-records", "100", "--target-rate", "100", "--output", str(Path(d) / "fixture.json"))
            summary = producer.run(a)
            self.assertEqual(summary["acknowledged_records"], 100)
            self.assertAlmostEqual(summary["actual_sent_interval_rate"], 100)
            self.assertEqual(sum(summary["acknowledged_partition_counts"].values()), 100)
            self.assertEqual(summary["max_schedule_lateness_seconds"], 0)
            with self.assertRaises(FileExistsError):
                producer.run(a)


class CleanupTest(unittest.TestCase):
    def test_cancel_actual_job_and_remove_only_this_producer(self):
        def cmd(argv, **kwargs):
            return "fixture-container\n" if argv[:3] == ["docker", "ps", "-aq"] else ""
        with patch.object(smoke_test, "command", side_effect=cmd) as command_mock, \
             patch.object(smoke_test, "rest", side_effect=[{"state": "RUNNING"}, None, {"state": "CANCELED"}]) as rest_mock:
            observations, errors = smoke_test.cleanup_run("http://fixture", "actual-id", "this-run", "this-topic", True)
        self.assertEqual(errors, [])
        self.assertEqual(observations["final_job_state"], "CANCELED")
        self.assertTrue(observations["topic_deleted_verified"])
        self.assertEqual(rest_mock.call_args_list[1].args[1], "/jobs/actual-id?mode=cancel")
        self.assertIn(["docker", "rm", "-f", "fixture-container"], [c.args[0] for c in command_mock.call_args_list])

    def test_cleanup_errors_preserved_and_other_cleanup_attempted(self):
        with patch.object(smoke_test, "command", side_effect=RuntimeError("fixture failure")), \
             patch.object(smoke_test, "rest", return_value={"state": "FAILED"}):
            observations, errors = smoke_test.cleanup_run("http://fixture", "actual-id", "this-run", "this-topic", True)
        self.assertEqual(observations["final_job_state"], "FAILED")
        self.assertEqual(len(errors), 2)


if __name__ == "__main__":
    unittest.main()
