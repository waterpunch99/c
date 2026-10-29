"""Source-only scaling on a fixed physical budget and a fixed downstream graph."""
import copy
import json
import math
from pathlib import Path
from lab import ROOT
from phase2_config import load_config, validate
from phase3_config import merge


def load_phase5(mode='smoke', path=None, overrides=None):
    if mode not in ['smoke', 'full']: raise ValueError('Unknown mode')
    data = json.loads((ROOT/'configs/experiments/phase5.json').read_text())
    if path: merge(data, json.loads(Path(path).read_text()))
    plan = copy.deepcopy(data[mode]); plan.update({k:v for k,v in (overrides or {}).items() if v is not None})
    if set(plan) != set(data[mode]): raise ValueError('Unknown Phase 5 override')
    cfg = load_config(mode=mode)
    for partial in [data['run_config'], plan.pop('run_overrides')]:
        for k,v in partial.items():
            if k not in cfg: raise ValueError('Unknown run option: '+k)
            if isinstance(cfg[k],dict):
                if not isinstance(v,dict) or set(v)-set(cfg[k]): raise ValueError('Unknown nested run option: '+k)
                cfg[k].update(v)
            else: cfg[k]=v
    downstream = data['downstream_parallelism']
    if set(downstream) != {'parse','heavy','aggregation','sink'}: raise ValueError('Expected fixed downstream graph')
    cfg['operator_parallelism'] = {'source':plan['parallelisms'][0] if plan['parallelisms'] else 1, **downstream}
    cfg['target_rate']=plan['target_rate']; validate(cfg)
    if cfg['distribution']!='uniform' or cfg['chaining']: raise ValueError('Source diagnosis requires uniform input and chaining off')
    ps=plan['parallelisms']
    if not isinstance(ps,list) or not ps or len(set(ps))!=len(ps) or any(type(p) is not int or not 1<=p<=cfg['max_parallelism'] for p in ps): raise ValueError('Invalid source parallelisms')
    if type(plan['repetitions']) is not int or plan['repetitions'] < (3 if mode=='full' else 1): raise ValueError('Full requires >=3 repetitions')
    if type(plan['random_order']) is not bool or type(plan['order_seed']) is not int or plan['order_seed']<0: raise ValueError('Invalid order policy')
    limit=data['maximum_downstream_busy_ms_per_second']
    if isinstance(limit,bool) or not isinstance(limit,(int,float)) or not math.isfinite(limit) or not 0<limit<=1000: raise ValueError('Invalid downstream limit')
    thresholds=json.loads((ROOT/'configs/experiments/phase4.json').read_text())['evidence_thresholds']
    return {'experiment':5,'mode':mode,'run_config':cfg,'plan':plan,'evidence_thresholds':thresholds,
            'maximum_downstream_busy_ms_per_second':limit}


def cost_plan(config, experiment=5):
    if experiment!=5: raise ValueError('Wrong experiment')
    n=len(config['plan']['parallelisms'])*config['plan']['repetitions']; cfg=config['run_config']
    return {'experiment':5,'mode':config['mode'],'maximum_run_count':n,
        'configured_load_and_drain_seconds_upper_bound':n*sum(cfg[k] for k in ['preflight_seconds','warmup_seconds','measurement_seconds','drain_timeout_seconds']),
        'note':'Setup/API/cleanup time excluded. Smoke observes assignment, not source capacity.'}
