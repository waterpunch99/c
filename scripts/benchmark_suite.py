"""Sequential experiments 1..5; immutable plans/manifest and stop on failed cleanup."""
import copy
import hashlib
import json
import random
import time
import uuid
from pathlib import Path
from lab import ROOT
from result_store import RunStore, read_jsonl


def configuration(experiment, mode, path=None, overrides=None):
    if experiment in [1,2]:
        from phase3_config import load_phase3, cost_plan
        from experiment_engine import execute
        cfg=load_phase3(mode,path,overrides)
    elif experiment in [3,4]:
        from phase4_config import load_phase4, cost_plan
        from phase4_engine import execute
        cfg=load_phase4(experiment,mode,path,overrides)
    elif experiment==5:
        from phase5_config import load_phase5, cost_plan
        from phase5_engine import execute
        cfg=load_phase5(mode,path,overrides)
    else: raise ValueError('Unknown experiment')
    return cfg,cost_plan(cfg,experiment),execute


def suite_plan(mode, path=None):
    if mode not in ['smoke','full']: raise ValueError('Unknown mode')
    data={'random_order':False,'order_seed':42,'experiments':{}}
    if path:
        supplied=json.loads(Path(path).read_text())
        if set(supplied)-set(data): raise ValueError('Unknown suite option')
        data.update(supplied)
    if type(data['random_order']) is not bool or type(data['order_seed']) is not int or data['order_seed']<0: raise ValueError('Invalid suite order')
    if not isinstance(data['experiments'],dict) or set(data['experiments'])-set('12345'): raise ValueError('Unknown suite experiment')
    plans=[]
    for experiment in range(1,6):
        entry=data['experiments'].get(str(experiment),{})
        if set(entry)-{'config','overrides'}: raise ValueError('Unknown experiment entry')
        config_path=entry.get('config')
        if config_path: config_path=ROOT/config_path
        cfg,cost,_=configuration(experiment,mode,config_path,entry.get('overrides'))
        plans.append({'experiment':experiment,'config':cfg,'cost':cost})
    if data['random_order']: random.Random(data['order_seed']).shuffle(plans)
    return {'mode':mode,'random_order':data['random_order'],'order_seed':data['order_seed'],'experiments':plans,
        'maximum_run_count':sum(p['cost']['maximum_run_count'] for p in plans),
        'configured_load_and_drain_seconds_upper_bound':sum(p['cost']['configured_load_and_drain_seconds_upper_bound'] for p in plans),
        'note':'Sequential, no simultaneous workload; setup/REST/cleanup additional. Full requires explicit CLI invocation.'}


def execute_suite(plan, output_dir, base='http://localhost:8081', executors=None):
    plan=copy.deepcopy(plan); directory=Path(output_dir)/'suites'/('suite_'+uuid.uuid4().hex)
    store=RunStore(directory); emit=store.jsonl('sessions.jsonl'); sessions=[]; errors=[]
    def artifacts():
        paths=[*(ROOT/'scripts').glob('*.py'),*(ROOT/'load-generator').glob('*.py'),ROOT/'flink-job/target/flink-parallelism-job.jar',ROOT/'docker-compose.yml']
        return {str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None for p in paths}
    hashes=artifacts()
    store.json('suite.json',{'started_wall_ms':time.time_ns()//1000000,'frozen_plan':plan,'artifact_sha256':hashes})
    try:
        for entry in plan['experiments']:
            experiment=entry['experiment']
            executor=(executors or {}).get(experiment)
            if executor is None: executor=configuration(experiment,plan['mode'])[2]
            session,summary=executor(entry['config'],experiment,output_dir,base)
            record={'experiment':experiment,'mode':plan['mode'],'session_directory':str(Path(session).resolve()),
                'session_relative_directory':str(Path(session).resolve().relative_to(Path(output_dir).resolve())),
                'session_id':summary['session_id'],'status':summary['status'],'run_count':summary['run_count']}
            sessions.append(record); emit(record)
            rows=read_jsonl(Path(session)/'runs.jsonl')
            if summary.get('errors') or any(r.get('cleanup_errors') for r in rows):
                raise RuntimeError('Session error/cleanup uncertainty; refusing next experiment: '+str(experiment))
            current=artifacts()
            if current!=hashes: raise RuntimeError('Script artifacts changed during suite; refusing condition mixing')
    except (Exception,KeyboardInterrupt) as exc: errors.append(repr(exc))
    finally:
        status='ERROR' if errors else 'COMPLETED' if len(sessions)==5 and all(s['status']=='COMPLETED' for s in sessions) else 'COMPLETED_WITH_RUN_FAILURES'
        summary={'status':status,'mode':plan['mode'],'sessions':sessions,'errors':errors,
            'not_executed_experiments':[p['experiment'] for p in plan['experiments'] if p['experiment'] not in {s['experiment'] for s in sessions}]}
        store.json('suite_summary.json',summary); store.close()
    print(json.dumps({'suite_directory':str(directory),**summary}),flush=True)
    return directory,summary
