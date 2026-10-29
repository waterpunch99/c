"""Experiments 1..5/all: isolated benchmarks; Full requires explicit invocation."""
import argparse
import json
import signal
from pathlib import Path
from lab import ROOT
from phase3_config import load_phase3, cost_plan
from experiment_engine import execute


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--experiment', choices=['1', '2', '3', '4', '5', 'all'], required=True)
    p.add_argument('--mode', choices=['smoke', 'full'], default='smoke'); p.add_argument('--config')
    p.add_argument('--repetitions', type=int); p.add_argument('--parallelisms', help='comma-separated positive integers')
    p.add_argument('--target-rate', type=float, help='fixed-input rate for experiment 1')
    p.add_argument('--peak-input-rate', type=float); p.add_argument('--target-utilization', type=float)
    p.add_argument('--initial-rate', type=float); p.add_argument('--max-rate', type=float); p.add_argument('--max-attempts', type=int)
    p.add_argument('--random-order', action='store_true', default=None)
    p.add_argument('--distribution', choices=['uniform','skewed'], help='experiment 4 only; omit to run paired distributions')
    p.add_argument('--hot-key-ratio', type=float); p.add_argument('--seed', type=int)
    p.add_argument('--output-dir', type=Path, default=ROOT / 'results')
    p.add_argument('--rest-url', default='http://localhost:8081'); p.add_argument('--dry-run', action='store_true')
    a = p.parse_args(); output = a.output_dir.resolve()
    if not output.is_relative_to(ROOT / 'results'): p.error('output-dir must be below results/ (Compose mount)')
    if a.experiment=='all':
        from benchmark_suite import suite_plan, execute_suite
        if any(v is not None for v in [a.repetitions,a.parallelisms,a.target_rate,a.peak_input_rate,a.target_utilization,a.initial_rate,a.max_rate,a.max_attempts,a.random_order,a.distribution,a.hot_key_ratio,a.seed]):
            p.error('For all, use the suite --config with per-experiment overrides; ambiguous global overrides rejected')
        plan=suite_plan(a.mode,a.config)
        if a.dry_run: print(json.dumps(plan,indent=2)); return 0
        signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(KeyboardInterrupt('SIGTERM')))
        _,result=execute_suite(plan,output,a.rest_url)
        return 0 if result['status']=='COMPLETED' else 1
    a.experiment=int(a.experiment)
    if a.experiment==5:
        from phase5_config import load_phase5, cost_plan as phase5_cost
        from phase5_engine import execute as phase5_execute
        if any(v is not None for v in [a.peak_input_rate,a.target_utilization,a.initial_rate,a.max_rate,a.max_attempts,a.distribution,a.hot_key_ratio,a.seed]): p.error('Experiment 5 accepts source parallelisms and uniform workload in config')
        overrides={'repetitions':a.repetitions,'target_rate':a.target_rate,'random_order':a.random_order}
        if a.parallelisms:
            try: overrides['parallelisms']=[int(v) for v in a.parallelisms.split(',')]
            except ValueError: p.error('Invalid parallelisms')
        cfg=load_phase5(a.mode,a.config,overrides)
        if a.dry_run: print(json.dumps({'plan':phase5_cost(cfg),'frozen_config':cfg},indent=2)); return 0
        signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(KeyboardInterrupt('SIGTERM')))
        _,result=phase5_execute(cfg,5,output,a.rest_url)
        return 0 if result['status']=='COMPLETED' else 1
    if a.experiment in [3,4]:
        from phase4_config import load_phase4, cost_plan as phase4_cost
        from phase4_engine import execute as phase4_execute
        if any(v is not None for v in [a.peak_input_rate,a.target_utilization,a.initial_rate,a.max_rate,a.max_attempts]): p.error('Capacity options apply to experiments 1/2')
        if a.experiment == 3 and (a.distribution is not None or a.parallelisms is not None): p.error('Experiment 3 uses named operator cases and uniform distribution')
        overrides = {'repetitions':a.repetitions,'target_rate':a.target_rate,'random_order':a.random_order,'seed':a.seed,'hot_key_ratio':a.hot_key_ratio}
        if a.parallelisms:
            try: overrides['parallelisms'] = [int(v) for v in a.parallelisms.split(',')]
            except ValueError: p.error('Invalid parallelisms')
        if a.distribution: overrides['distributions'] = [a.distribution]
        cfg = load_phase4(a.experiment,a.mode,a.config,overrides)
        if a.dry_run:
            print(json.dumps({'plan':phase4_cost(cfg,a.experiment),'frozen_config':cfg},indent=2)); return 0
        signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(KeyboardInterrupt('SIGTERM')))
        _,result = phase4_execute(cfg,a.experiment,output,a.rest_url)
        return 0 if result['status']=='COMPLETED' else 1
    if a.distribution is not None or a.hot_key_ratio is not None or a.seed is not None: p.error('Distribution/seed options currently apply to experiments 3/4')
    overrides = {'repetitions': a.repetitions, 'fixed_input_rate': a.target_rate, 'peak_input_rate': a.peak_input_rate,
                 'target_utilization': a.target_utilization, 'initial_rate': a.initial_rate, 'max_rate': a.max_rate,
                 'max_attempts': a.max_attempts, 'random_order': a.random_order}
    if a.parallelisms:
        try: values = [int(v) for v in a.parallelisms.split(',')]
        except ValueError: p.error('Invalid parallelisms')
        overrides['parallelisms' if a.experiment == 1 else 'calibration_parallelisms'] = values
    cfg = load_phase3(a.mode, a.config, overrides)
    if a.dry_run:
        print(json.dumps({'plan': cost_plan(cfg, a.experiment), 'frozen_config': cfg}, indent=2)); return 0
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt('SIGTERM')))
    _, result = execute(cfg, a.experiment, output, a.rest_url)
    return 0 if result['status'] == 'COMPLETED' else 1


if __name__ == '__main__':
    raise SystemExit(main())
