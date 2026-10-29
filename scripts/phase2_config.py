import copy
import json
import math
from lab import ROOT


def load_config(path=None, mode='smoke', overrides=None):
    default = json.loads((ROOT / 'configs/experiments/phase2.json').read_text())
    cfg = copy.deepcopy(default)
    cfg.setdefault('operator_cpu_sample_every', 0)
    if mode == 'smoke':
        cfg.update(warmup_seconds=5, measurement_seconds=20, sample_interval_seconds=2,
                   checkpoint_interval_ms=1000)
    if path:
        supplied = json.loads(open(path, encoding='utf-8').read())
        if set(supplied) - set(cfg):
            raise ValueError('Unknown config options: ' + str(set(supplied) - set(cfg)))
        for key, value in supplied.items():
            if key in ['thresholds', 'operator_parallelism']:
                if set(value) - set(cfg[key]):
                    raise ValueError('Unknown nested config keys: ' + key)
                cfg[key].update(value)
            else:
                cfg[key] = value
    cfg.update({k: v for k, v in (overrides or {}).items() if v is not None})
    validate(cfg)
    return cfg


def validate(cfg):
    if cfg['resource_mode'] != 'fixed-resource' or cfg['producer_workers'] != 1:
        raise ValueError('Phase 2 supports fixed-resource and one producer worker only')
    if type(cfg.get('kafka_probe_attempts',2)) is not int or not 1<=cfg.get('kafka_probe_attempts',2)<=3:
        raise ValueError('kafka_probe_attempts must be 1..3')
    for name in ['target_rate', 'measurement_seconds', 'sample_interval_seconds', 'preflight_seconds', 'drain_timeout_seconds']:
        if isinstance(cfg[name], bool) or not isinstance(cfg[name], (int, float)) or not math.isfinite(cfg[name]) or cfg[name] <= 0:
            raise ValueError('Expected finite positive ' + name)
    for name in ['kafka_partitions', 'number_of_users', 'max_parallelism', 'checkpoint_interval_ms', 'checkpoint_timeout_ms']:
        if type(cfg[name]) is not int or cfg[name] < 1:
            raise ValueError('Expected positive integer ' + name)
    for name in ['payload_size', 'cpu_work_iterations', 'keyed_cpu_work_iterations', 'latency_sample_every', 'operator_cpu_sample_every']:
        if name == 'operator_cpu_sample_every' and name not in cfg: continue
        if type(cfg[name]) is not int or cfg[name] < 0:
            raise ValueError('Expected nonnegative integer ' + name)
    if not isinstance(cfg['warmup_seconds'], (int, float)) or not math.isfinite(cfg['warmup_seconds']) or cfg['warmup_seconds'] < 0:
        raise ValueError('Invalid warmup_seconds')
    if cfg['target_rate'] * cfg['measurement_seconds'] < 1 or cfg['number_of_users'] < 2 or cfg['max_parallelism'] > 32768:
        raise ValueError('Invalid record count/users/maxParallelism')
    if cfg['distribution'] not in ['uniform', 'skewed'] or not 0 <= cfg['hot_key_ratio'] <= 1:
        raise ValueError('Invalid distribution/hot ratio')
    if cfg['partition_strategy'] not in ['round-robin', 'random'] or cfg['state_backend'] not in ['hashmap', 'rocksdb'] or type(cfg['chaining']) is not bool:
        raise ValueError('Invalid partition strategy/state backend/chaining')
    for p in cfg['operator_parallelism'].values():
        if type(p) is not int or p < 1 or p > cfg['max_parallelism']:
            raise ValueError('Parallelism out of range')
    # Phase 2 uses unchained diagnostics. Chained task metrics cannot be attributed to every operator independently.
    if cfg['chaining']:
        raise ValueError('Phase 2 collector requires chaining=false; pipeline still supports manual chaining')
    for name, value in cfg['thresholds'].items():
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise ValueError('Invalid threshold: ' + name)
    for name in ['producer_rate_tolerance', 'minimum_interval_rate_ratio', 'minimum_sample_coverage',
                 'sink_produced_ratio', 'maximum_cpu_quota_utilization', 'maximum_throttled_period_ratio']:
        if cfg['thresholds'][name] > 1:
            raise ValueError('Ratio threshold out of range: ' + name)
