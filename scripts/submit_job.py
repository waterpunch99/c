"""Submit a Phase 1 job and emit its actual Flink Job ID."""
import argparse
import re
from lab import command, rest, wait_for


def submit(base, job_args):
    output = command(["docker", "compose", "exec", "-T", "jobmanager", "/opt/flink/bin/flink", "run", "-d",
                      "/opt/flink/usrlib/flink-parallelism-job.jar", *job_args], timeout=120)
    match = re.search(r"Job has been submitted with JobID ([a-fA-F0-9]{32})", output)
    if not match:
        raise RuntimeError("Could not identify actual submitted Job ID:\n" + output)
    return match.group(1)


def wait_running(base, job_id):
    def running():
        data = rest(base, "/jobs/" + job_id)
        state = data["state"]
        if state in {"FAILED", "CANCELED", "FINISHED", "SUSPENDED"}:
            raise RuntimeError(f"Job terminated before readiness: {state}")
        return data if state == "RUNNING" and all(v["status"] == "RUNNING" for v in data["vertices"]) else None
    return wait_for(running, "all Job vertices RUNNING")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rest-url", default="http://localhost:8081")
    known, extra = p.parse_known_args()
    if extra and extra[0] == "--":
        extra = extra[1:]
    jid = submit(known.rest_url, extra)
    print(jid, flush=True)
    wait_running(known.rest_url, jid)
