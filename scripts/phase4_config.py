"""Phase 4 fixed-resource cases, paired distribution policy and cost plan."""
import copy
import json
import math
from pathlib import Path
from lab import ROOT
from phase2_config import load_config, validate
from phase3_config import merge

OPERATORS = {'source', 'parse', 'heavy', 'aggregation', 'sink'}


def load_phase4(experiment, mode='smoke', path=None, overrides=None):
    if experiment not in [3, 4] or mode not in ['smoke', 'full']: raise ValueError('Phase 4 supports experiments 3/4')
    data = json.loads((ROOT / 'configs/experiments/phase4.json').read_text())
    if path: merge(data, json.loads(Path(path).read_text(encoding='utf-8')))
    plan = copy.deepcopy(data[mode]); plan.update({k: v for k, v in (overrides or {}).items() if v is not None and k in plan})
    if set(overrides or {}) - (set(plan) | {'seed', 'hot_key_ratio'}): raise ValueError('Unknown Phase 4 override')
    cfg = load_config(mode=mode)
    for partial in [data['run_config'], plan.pop('run_overrides')]:
        for k, v in partial.items():
            if k not in cfg: raise ValueError('Unknown run option: ' + k)
            if isinstance(cfg[k], dict):
                if not isinstance(v, dict) or set(v) - set(cfg[k]): raise ValueError('Invalid run option: ' + k)
                cfg[k].update(v)
            else: cfg[k] = v
    workload = copy.deepcopy(data['experiment_' + str(experiment)])
    cfg.update({k: workload[k] for k in ['cpu_work_iterations', 'keyed_cpu_work_iterations']})
    cfg.update({k: v for k, v in (overrides or {}).items() if k in ['seed', 'hot_key_ratio'] and v is not None})
    cfg['target_rate'] = plan['target_rate']; cfg['distribution'] = 'uniform'
    validate(cfg)
    if type(cfg['seed']) is not int or isinstance(cfg['hot_key_ratio'], bool) or not math.isfinite(cfg['hot_key_ratio']): raise ValueError('Invalid seed/hot ratio')
    if type(plan['repetitions']) is not int or plan['repetitions'] < (3 if mode == 'full' else 1): raise ValueError('Full requires >=3 repetitions')
    if type(plan['order_seed']) is not int or plan['order_seed'] < 0 or type(plan['random_order']) is not bool: raise ValueError('Invalid order policy')
    ps = plan['parallelisms']; distributions = plan['distributions']
    if not isinstance(ps, list) or not ps or any(type(p) is not int or not 1 <= p <= cfg['max_parallelism'] for p in ps) or len(set(ps)) != len(ps): raise ValueError('Invalid parallelisms')
    if not isinstance(distributions, list) or not distributions or len(set(distributions)) != len(distributions) or set(distributions) - {'uniform', 'skewed'}: raise ValueError('Invalid distributions')
    if experiment == 3:
        if set(workload['cases']) != {'low_baseline', 'uniform_high', 'bottleneck_focused'}: raise ValueError('Expected three named cases')
        for case in workload['cases'].values():
            if set(case) != OPERATORS or any(type(p) is not int or not 1 <= p <= cfg['max_parallelism'] for p in case.values()): raise ValueError('Invalid case operator parallelism')
    for k,v in data['evidence_thresholds'].items():
        if isinstance(v, bool) or not isinstance(v, (int,float)) or not math.isfinite(v) or v < 0: raise ValueError('Invalid threshold: ' + k)
    for k in ['maximum_hot_ratio_absolute_error', 'maximum_partition_total_variation', 'precheck_minimum_sink_ratio', 'maximum_cpu_quota_utilization', 'maximum_throttled_period_ratio']:
        if data['evidence_thresholds'][k] > 1: raise ValueError('Invalid ratio: ' + k)
    if type(data['evidence_thresholds']['minimum_distribution_records']) is not int or data['evidence_thresholds']['minimum_distribution_records'] < 1: raise ValueError('Invalid minimum distribution sample')
    return {'experiment': experiment, 'mode': mode, 'run_config': cfg, 'plan': plan,
            'workload': workload, 'evidence_thresholds': data['evidence_thresholds']}


def cost_plan(config, experiment):
    p = config['plan']; cfg = config['run_config']
    count = (4 if experiment == 3 else len(p['parallelisms']) * len(p['distributions'])) * p['repetitions']
    return {'experiment': experiment, 'mode': config['mode'], 'maximum_run_count': count,
            'configured_load_and_drain_seconds_upper_bound': count * sum(cfg[k] for k in
             ['preflight_seconds', 'warmup_seconds', 'measurement_seconds', 'drain_timeout_seconds']),
            'note': 'Experiment 3 includes low-work prechecks. Setup/API/cleanup time excluded; Smoke not capacity evidence.'}
