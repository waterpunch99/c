"""Frozen session configuration and a conservative upper bound on run cost."""
import copy
import json
import math
from lab import ROOT
from phase2_config import load_config, validate


def merge(dst, src):
    for k, v in src.items():
        if k not in dst:
            raise ValueError('Unknown option: ' + k)
        if isinstance(dst[k], dict):
            if not isinstance(v, dict):
                raise ValueError('Expected object: ' + k)
            # Run config dictionaries are partial Phase 2 overrides, checked by validate below.
            if k in ['run_config', 'run_overrides']:
                dst[k].update(v)
            else:
                merge(dst[k], v)
        else:
            dst[k] = v


def load_phase3(mode='smoke', path=None, overrides=None):
    data = json.loads((ROOT / 'configs/experiments/phase3.json').read_text())
    if path:
        merge(data, json.loads(open(path, encoding='utf-8').read()))
    plan = copy.deepcopy(data[mode]); plan.update({k: v for k, v in (overrides or {}).items() if v is not None})
    cfg = load_config(mode=mode)
    for partial in [data['run_config'], plan.pop('run_overrides')]:
        for k, v in partial.items():
            if k not in cfg:
                raise ValueError('Unknown run option: ' + k)
            if isinstance(cfg[k], dict):
                if set(v) - set(cfg[k]): raise ValueError('Unknown nested run option: ' + k)
                cfg[k].update(v)
            else: cfg[k] = v
    validate(cfg)
    if cfg['distribution'] != 'uniform':
        raise ValueError('Phase 3 calibration uses uniform workload; skew experiment belongs to Phase 4')
    for name in ['parallelisms', 'calibration_parallelisms', 'uncalibrated_probe_parallelisms']:
        if not isinstance(plan[name], list) or len(plan[name]) != len(set(plan[name])) or (not plan[name] and name != 'uncalibrated_probe_parallelisms'):
            raise ValueError('Expected unique nonempty list: ' + name)
        if any(type(p) is not int or p < 1 or p > min(plan['max_test_parallelism'], cfg['max_parallelism']) for p in plan[name]):
            raise ValueError('Invalid parallelism: ' + name)
    for name in ['repetitions', 'max_attempts', 'max_test_parallelism', 'order_seed']:
        if type(plan[name]) is not int or plan[name] < (0 if name == 'order_seed' else 1): raise ValueError('Invalid ' + name)
    if mode == 'full' and plan['repetitions'] < 3:
        raise ValueError('Full mode requires >=3 repetitions')
    for name in ['minimum_rate', 'initial_rate', 'max_rate', 'growth_factor', 'rate_relative_resolution', 'fixed_input_rate', 'peak_input_rate', 'target_utilization']:
        v = plan[name]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0: raise ValueError('Invalid ' + name)
    if not plan['minimum_rate'] <= plan['initial_rate'] <= plan['max_rate'] or plan['growth_factor'] <= 1 or plan['rate_relative_resolution'] > 1 or plan['target_utilization'] > 1:
        raise ValueError('Invalid search bounds/utilization')
    if type(plan['random_order']) is not bool: raise ValueError('random_order must be bool')
    if set(plan) - (set(data[mode]) - {'run_overrides'}):
        raise ValueError('Unknown plan override')
    for section in ['evidence_thresholds', 'operational_policy']:
        for k, v in data[section].items():
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0: raise ValueError('Invalid ' + k)
    minimum = data['evidence_thresholds']['minimum_calibration_parallelisms']
    if type(minimum) is not int or minimum < 2: raise ValueError('Calibration evidence requires >=2 distinct parallelisms')
    if data['evidence_thresholds']['maximum_capacity_bracket_relative_width'] > 1: raise ValueError('Invalid capacity resolution')
    return {'mode': mode, 'run_config': cfg, 'plan': plan, 'evidence_thresholds': data['evidence_thresholds'],
            'operational_policy': data['operational_policy']}


def cost_plan(config, experiment):
    p = config['plan']; cfg = config['run_config']
    discovery = len(p['parallelisms'] if experiment == 1 else p['calibration_parallelisms']) * p['max_attempts'] * p['repetitions']
    secondary = len(p['parallelisms']) * p['repetitions'] if experiment == 1 else (len(p['uncalibrated_probe_parallelisms']) if config['mode'] == 'smoke' else 5) * p['repetitions']
    # Setup/API/healthcheck/cleanup latency is additional and environment-dependent.
    return {'experiment': experiment, 'mode': config['mode'], 'maximum_run_count': discovery + secondary,
            'maximum_discovery_runs': discovery, 'maximum_secondary_runs': secondary,
            'configured_load_and_drain_seconds_upper_bound': (discovery + secondary) * (cfg['preflight_seconds'] + cfg['warmup_seconds'] + cfg['measurement_seconds'] + cfg['drain_timeout_seconds']),
            'note': 'Adaptive search can stop early. Setup/API/cleanup time excluded. Smoke never estimates sustainable capacity.'}
