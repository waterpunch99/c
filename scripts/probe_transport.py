"""Machine JSON transport: retain diagnostics, bounded empty/malformed-response retry."""
import json
import subprocess
import time
from lab import ROOT


def query_json(args, timeout=20, attempts=2, diagnostics=None):
    if type(attempts) is not int or not 1<=attempts<=3: raise ValueError('Invalid probe attempts')
    for attempt in range(1,attempts+1):
        result=subprocess.run(args,cwd=ROOT,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
        error=None; value=None
        if result.returncode: error='Command failed: '+str(result.returncode)
        else:
            try:
                value=json.loads(result.stdout)
                if not isinstance(value,dict): error='Expected JSON object'
            except ValueError as exc: error='Missing/malformed JSON: '+repr(exc)
        if diagnostics and (error or result.stderr):
            diagnostics({'wall_time_ms':time.time_ns()//1000000,'attempt':attempt,'command':args,
                'returncode':result.returncode,'stdout':result.stdout,'stderr':result.stderr,'error':error})
        if error is None: return value
        if result.returncode or attempt==attempts:
            raise RuntimeError(f'Kafka probe transport: {error}; stdout={result.stdout[:500]!r}; stderr={result.stderr[:500]!r}')
    raise AssertionError('Unreachable')
