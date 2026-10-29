"""Container cgroup v2 observations; unsupported files remain null with reasons."""
import json
import os
import time
from pathlib import Path


def probe(root=Path('/sys/fs/cgroup')):
    result = {"wall_time_ms": time.time_ns() // 1000000, "cpu_affinity_count": len(os.sched_getaffinity(0)), "missing": {}}
    for name in ['cpu.max', 'cpu.stat', 'memory.current', 'memory.max', 'memory.events']:
        try:
            raw = (root / name).read_text().strip()
            if name in ['cpu.stat', 'memory.events']:
                result[name] = {k: int(v) for k, v in (line.split() for line in raw.splitlines())}
            else:
                result[name] = raw
        except (OSError, ValueError) as exc:
            result[name] = None
            result['missing'][name] = repr(exc)
    return result


if __name__ == '__main__':
    print(json.dumps(probe()))
