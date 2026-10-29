"""Real Kafka/Flink bounded integration test; no capacity claims or fabricated samples."""
import argparse
import json
import os
import platform
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from lab import ROOT, command, rest, wait_for, snapshot_subtasks, audit_sink, save_json, find_metric
from submit_job import submit, wait_running


def cleanup_run(base, job_id, producer_name, topic, delete_topic):
    observations = {}
    errors = []
    try:
        ids = command(["docker", "ps", "-aq", "--filter", f"name=^/{producer_name}$"]).split()
        if ids:
            command(["docker", "rm", "-f", *ids])
        observations["producer_container_removed"] = True
    except Exception as exc:
        errors.append("producer cleanup: " + repr(exc))
    if job_id:
        try:
            state = rest(base, "/jobs/" + job_id)["state"]
            if state not in {"FAILED", "CANCELED", "FINISHED"}:
                rest(base, "/jobs/" + job_id + "?mode=cancel", method="PATCH")
            observations["final_job_state"] = wait_for(lambda: s if (s := rest(base, "/jobs/" + job_id)["state"]) in
                                                {"FAILED", "CANCELED", "FINISHED"} else None, "Job termination", 60)
        except Exception as exc:
            errors.append("job cleanup: " + repr(exc))
    if delete_topic:
        try:
            command(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                     "--bootstrap-server", "kafka:29092", "--delete", "--topic", topic])
            observations["topic_delete_requested"] = True
            wait_for(lambda: topic not in command(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
                     "--bootstrap-server", "kafka:29092", "--list"]).splitlines(), "Topic deletion", 30)
            observations["topic_deleted_verified"] = True
        except Exception as exc:
            errors.append("topic cleanup: " + repr(exc))
    return observations, errors


def clock_probe(service):
    before = time.time_ns() // 1_000_000
    observed = int(command(["docker", "compose", "exec", "-T", service, "date", "+%s%3N"]).strip())
    after = time.time_ns() // 1_000_000
    return {"container_minus_host_ms_lower": observed - after,
            "container_minus_host_ms_upper": observed - before,
            "round_trip_ms": after - before, "method": "host timestamp bracket; uncertainty retained"}


def run(a):
    run_id = "smoke_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8]
    directory = ROOT / "results" / "smoke" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    topic = "lab-" + run_id.replace("_", "-")
    group = "group-" + run_id
    producer_name = "producer-" + run_id
    job_id = None
    topic_created = False
    rows = []
    error = None
    cleanup_errors = []
    metadata = {"kind": "phase1_integration_smoke", "run_id": run_id, "experiment_id": "phase1",
                "timestamp": datetime.now(timezone.utc).isoformat(), "topic": topic, "group_id": group,
                "host": {"os": platform.platform(), "architecture": platform.machine(),
                         "python": platform.python_version(), "cpu_count": os.cpu_count(),
                         "available_cpu_affinity": len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None},
                "resource_mode": "fixed-resource", "state_backend": a.state_backend,
                "max_parallelism": 128, "operator_chaining": False,
                "checkpoint_interval_ms": 1000, "checkpoint_timeout_ms": 60000,
                "cpu_work_iterations": 500, "keyed_cpu_work_iterations": 100,
                "latency_sample_every": 10, "verification_records": a.records,
                "operator_parallelism": {"source": 2, "parse": 3, "heavy": 4, "aggregation": 2, "sink": 3},
                "kafka_partitions": a.partitions, "target_rate": a.target_rate,
                "limitations": ["Smoke is not Sustainable Capacity evidence", "No restart/exactly-once sink claim",
                                "Full environment/metrics, phase windows and stability decisions belong to Phase 2"]}
    summary = {"execution_validity": "ERROR", "performance_stability": "UNKNOWN", "capacity_estimate": None}
    try:
        try:
            metadata["git_commit_sha"] = command(["git", "rev-parse", "--verify", "HEAD"]).strip()
        except RuntimeError:
            metadata["git_commit_sha"] = None
        metadata["compose_config"] = json.loads(command(["docker", "compose", "config", "--format", "json"]))
        command(["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "180", "kafka", "jobmanager", "taskmanager"], 240)
        wait_for(lambda: (o if (o := rest(a.rest_url, "/overview"))["slots-available"] >= 4 and o["taskmanagers"] >= 1 else None),
                 "TaskManager registration and at least four free slots")
        metadata["flink_overview"] = rest(a.rest_url, "/overview")
        metadata["taskmanagers"] = rest(a.rest_url, "/taskmanagers")
        metadata["clock_probes"] = {s: clock_probe(s) for s in ["kafka", "jobmanager", "taskmanager"]}
        metadata["docker_info"] = json.loads(command(["docker", "info", "--format", "{{json .}}"]));
        metadata["flink_java_version"] = command(["docker", "compose", "exec", "-T", "jobmanager", "java", "-version"]).strip()
        metadata["container_inspect"] = json.loads(command(["docker", "inspect", *command([
            "docker", "compose", "ps", "-q", "kafka", "jobmanager", "taskmanager"]).split()]))
        for tm in metadata["taskmanagers"]["taskmanagers"]:
            tm["details"] = rest(a.rest_url, "/taskmanagers/" + tm["id"])
        kafka_version = command(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--version"])
        metadata["kafka_version"] = kafka_version.strip()
        command(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092",
                 "--create", "--topic", topic, "--partitions", str(a.partitions), "--replication-factor", "1"])
        topic_created = True
        metadata["topic_description"] = command(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
              "--bootstrap-server", "kafka:29092", "--describe", "--topic", topic])
        job_args = ["--topic", topic, "--group-id", group, "--experiment-id", "phase1", "--run-id", run_id,
                    "--cpu-work-iterations", "500", "--keyed-cpu-work-iterations", "100",
                    "--checkpoint-interval-ms", "1000", "--latency-sample-every", "10",
                    "--verification-records", str(a.records), "--state-backend", a.state_backend,
                    "--source-parallelism", "2", "--parse-parallelism", "3", "--heavy-parallelism", "4",
                    "--aggregation-parallelism", "2", "--sink-parallelism", "3"]
        metadata["job_args"] = job_args
        metadata["checkpoint_path"] = "file:///checkpoints/" + run_id
        job_id = submit(a.rest_url, job_args)
        metadata["job_id"] = job_id
        job = wait_running(a.rest_url, job_id)
        expected_parallelism = {"Kafka Source": 2, "Parse": 3, "Heavy CPU": 4, "Stateful Aggregation": 2, "Measurement Sink": 3}
        for name, expected in expected_parallelism.items():
            matching = [v for v in job["vertices"] if name in v["name"]]
            if len(matching) != 1 or matching[0]["parallelism"] != expected:
                raise RuntimeError(f"Actual Job graph does not match configured parallelism for {name}")
        metadata["job_graph"] = job
        rel = directory.relative_to(ROOT / "results")
        output = command(["docker", "compose", "run", "--rm", "--no-deps", "--name", producer_name, "producer",
            "--topic", topic, "--experiment-id", "phase1", "--run-id", run_id,
            "--total-records", str(a.records), "--target-rate", str(a.target_rate),
            "--kafka-partitions", str(a.partitions), "--output", "/results/" + str(rel / "producer.json")], 100)
        (directory / "producer.log").write_text(output, encoding="utf-8")
        producer = json.loads((directory / "producer.json").read_text())
        if producer["acknowledged_records"] != a.records:
            raise RuntimeError("Producer ACK count mismatch")
        # Source/sink readiness is demonstrated by real data progress, not assumed from Job RUNNING.
        available = {}
        def drained():
            nonlocal rows, available
            state = rest(a.rest_url, "/jobs/" + job_id)["state"]
            if state != "RUNNING":
                raise RuntimeError("Job left RUNNING during processing: " + state)
            rows, available = snapshot_subtasks(a.rest_url, job_id, job["vertices"], available)
            try:
                audit = audit_sink(rows, a.records)
                if audit:
                    audit["parse_errors"] = sum(find_metric(r["metrics"], "parseErrors") for r in rows if "Parse" in r["operator"])
                summary.pop("missing_metrics", None)
                return audit
            except RuntimeError as exc:
                if str(exc).startswith("Missing/ambiguous metric:"):
                    summary["missing_metrics"] = [str(exc)]
                    return None
                raise
        audit = wait_for(drained, "complete exact eventId set at sink", timeout=a.drain_timeout)
        def checkpointed():
            c = rest(a.rest_url, "/jobs/" + job_id + "/checkpoints")
            return c if c["counts"]["completed"] > 0 else None
        checkpoints = wait_for(checkpointed, "a completed real checkpoint", timeout=60)
        parse_errors = audit["parse_errors"]
        if parse_errors:
            raise RuntimeError("Parse errors observed")
        save_json(directory / "subtasks.json", rows)
        save_json(directory / "checkpoints.json", checkpoints)
        summary.update(audit)
        summary.update({"execution_validity": "VALID", "parse_errors": parse_errors,
                        "job_id": job_id, "producer_acknowledged_records": producer["acknowledged_records"],
                        "completed_checkpoints": checkpoints["counts"]["completed"],
                        "reason": "Real Kafka ACKs, exact bounded eventId audit, configured graph and checkpoint verified",
                        "stability_reason": "Short Phase 1 integration Smoke; use run_once.py for Phase 2 stability assessment"})
    except (Exception, KeyboardInterrupt) as exc:
        error = repr(exc)
        summary["execution_validity"] = "ERROR"
        summary["error"] = error
        if job_id:
            try:
                save_json(directory / "job-exceptions.json", rest(a.rest_url, f"/jobs/{job_id}/exceptions"))
            except Exception as secondary:
                summary["exception_collection_error"] = repr(secondary)
    finally:
        observations, cleanup_errors = cleanup_run(a.rest_url, job_id, producer_name, topic, topic_created and not a.keep_topic)
        summary.update(observations)
        summary["cleanup_errors"] = cleanup_errors
        if cleanup_errors:
            summary["execution_validity"] = "ERROR"
        if rows and not (directory / "subtasks.json").exists():
            save_json(directory / "subtasks.json", rows)
        save_json(directory / "metadata.json", metadata)
        save_json(directory / "summary.json", summary)
        print(json.dumps({"result_dir": str(directory), **summary}, ensure_ascii=False, indent=2), flush=True)
    if error or cleanup_errors:
        raise SystemExit(1)
    return directory


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rest-url", default="http://localhost:8081")
    p.add_argument("--records", type=int, default=600)
    p.add_argument("--target-rate", type=float, default=200)
    p.add_argument("--partitions", type=int, default=8)
    p.add_argument("--drain-timeout", type=int, default=60)
    p.add_argument("--state-backend", choices=["hashmap", "rocksdb"], default="hashmap")
    p.add_argument("--keep-topic", action="store_true")
    args = p.parse_args()
    import math
    if not 1 <= args.records <= 10000 or args.partitions < 1 or args.drain_timeout < 1 or not math.isfinite(args.target_rate) or args.target_rate <= 0:
        p.error("records: 1..10000; partitions, rate and drain timeout must be positive")
    run(args)
