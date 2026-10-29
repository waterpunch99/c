"""Phase 1 Docker/REST helpers, with explicit missing values and bounded waits."""
import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(args, timeout=120):
    result = subprocess.run(args, cwd=ROOT, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {args}\n{result.stdout}")
    return result.stdout


def rest(base, path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base.rstrip("/") + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def wait_for(fn, description, timeout=90):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except urllib.error.HTTPError as exc:
            if exc.code < 500:
                raise
            last = repr(exc)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last = repr(exc)
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for {description}; last transport error={last}")


def numeric_metric(value):
    if value is None:
        return None
    try:
        # Preserve 64-bit integer precision for offsets, counts, audit masks and checksums.
        return int(value)
    except (TypeError, ValueError):
        try:
            number = float(value)
            import math
            return number if math.isfinite(number) else None
        except (TypeError, ValueError):
            return None


def find_metric(metrics, suffix):
    matches = [numeric_metric(v) for k, v in metrics.items() if k == suffix or k.endswith("." + suffix)]
    if len(matches) != 1 or matches[0] is None:
        raise RuntimeError(f"Missing/ambiguous metric: {suffix}")
    return matches[0]


def snapshot_subtasks(base, job_id, vertices, available=None):
    available = {} if available is None else available
    rows = []
    for vertex in vertices:
        vid = vertex["id"]
        details = rest(base, f"/jobs/{job_id}/vertices/{vid}")
        for st in details["subtasks"]:
            i = st["subtask"]
            path = f"/jobs/{job_id}/vertices/{vid}/subtasks/{i}/metrics"
            key = (vid, i)
            # Flink's metric inventory is asynchronous and source/operator metrics can appear later.
            # Refresh it; an early missing counter must not remain missing in a cached inventory.
            available[key] = [m["id"] for m in rest(base, path)]
            ids = available[key]
            result = []
            for query in metric_query_paths(path, ids):
                result.extend(rest(base, query))
            rows.append({"vertex_id": vid, "operator": vertex["name"], "subtask": i,
                         "taskmanager_id": st.get("taskmanager-id"), "host": st.get("endpoint", st.get("host")),
                         "status": st["status"], "parallelism": vertex["parallelism"],
                         "metrics": {m["id"]: m.get("value") for m in result}})
    return rows, available


def metric_query_paths(path, ids, max_length=3000):
    import urllib.parse
    batch = []
    for metric in ids:
        candidate = path + "?get=" + urllib.parse.quote(",".join([*batch, metric]), safe="")
        if len(candidate) > max_length:
            if not batch:
                raise ValueError("Single metric ID exceeds safe REST request length")
            yield path + "?get=" + urllib.parse.quote(",".join(batch), safe="")
            batch = [metric]
            if len(path + "?get=" + urllib.parse.quote(metric, safe="")) > max_length:
                raise ValueError("Single metric ID exceeds safe REST request length")
        else:
            batch.append(metric)
    if batch:
        yield path + "?get=" + urllib.parse.quote(",".join(batch), safe="")


def audit_sink(rows, expected):
    sinks = [r for r in rows if "Measurement Sink" in r["operator"]]
    if not sinks:
        raise RuntimeError("No measured sink subtasks")
    count = sum(find_metric(r["metrics"], "completedRecords") for r in sinks)
    if count != expected:
        return None
    duplicate_count = sum(find_metric(r["metrics"], "duplicateRecords") for r in sinks)
    words = [0] * ((expected + 63) // 64)
    overlap = 0
    for r in sinks:
        for i in range(len(words)):
            bits = int(find_metric(r["metrics"], f"verificationSeenWord{i}")) & ((1 << 64) - 1)
            overlap += (words[i] & bits).bit_count()
            words[i] |= bits
    full = all(word == (1 << min(64, expected - i * 64)) - 1 for i, word in enumerate(words))
    seq_sum = sum(find_metric(r["metrics"], "sequenceSum") for r in sinks)
    if duplicate_count or overlap or not full or seq_sum != expected * (expected - 1) // 2:
        raise RuntimeError("Exact bounded eventId audit failed: duplicate/missing event")
    return {"completed_records": count, "unique_event_ids": sum(x.bit_count() for x in words),
            "duplicate_records": duplicate_count + overlap, "sequence_sum": seq_sum,
            "audit_scope": f"all {expected} Smoke eventIds (bounded bitsets, merged across subtasks)"}


def save_json(path, value):
    with Path(path).open("x", encoding="utf-8") as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
        f.write("\n")
