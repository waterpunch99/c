"""Experiments 3/4 only, isolated real runs and evidence-qualified comparisons."""
import copy
import json
import os
import random
import time
import uuid
from run_once import run
from result_store import RunStore
from analyze_results import flatten, write_csv
from phase4_config import cost_plan
from phase4_diagnostics import phase4_diagnose, paired_partition_comparison, precheck_decision


def comparison_summary(rows, diagnostics, cfg, precheck):
    experiment = cfg['experiment']; result = {'pairs': [], 'cases': [], 'precheck': precheck}
    for r in rows:
        d = diagnostics[r['run_id']]
        target = 'heavy' if experiment == 3 else 'aggregation'
        result['cases'].append({'run_id': r['run_id'], 'case': r['case'], 'distribution': r['distribution'],
            'role': r['role'], 'parallelism': r['parallelism'], 'operator_parallelism': r['operator_parallelism'],
            'execution_validity': r['execution_validity'], 'performance_stability': r['performance_stability'],
            'input_comparison_validity': d['input_validation']['status'],
            'expected_bottleneck': target, 'observed_bottleneck_candidate': d['bottleneck_candidate'],
            'expected_bottleneck_observed': r['role'] != 'low_work_precheck' and r['execution_validity'] == 'VALID' and d['input_validation']['status'] == 'VALID' and d['bottleneck_candidate'] == target and d['aggregation_load']['placement_status'] != 'CONTAMINATED_MULTIPLE_SUBTASKS',
            'resource_warnings': d['resource_saturation'], 'hot_placement': d['aggregation_load']['placement_status']})
    if experiment == 4:
        for p in cfg['plan']['parallelisms']:
            for repeat in range(cfg['plan']['repetitions']):
                pair = {r['distribution']: r for r in rows if r['parallelism'] == p and r['repetition'] == repeat}
                if set(pair) != {'uniform', 'skewed'}:
                    result['pairs'].append({'parallelism': p, 'repetition': repeat, 'status': 'Not Executed', 'reason': 'Uniform/Skewed pair incomplete'}); continue
                a,b = pair['uniform'], pair['skewed']
                check = paired_partition_comparison(diagnostics[a['run_id']], diagnostics[b['run_id']], cfg['evidence_thresholds']['maximum_partition_total_variation'])
                result['pairs'].append({'parallelism': p, 'repetition': repeat, 'uniform_run_id': a['run_id'], 'skewed_run_id': b['run_id'], **check})
    else:
        eligible = [r for r in rows if r['role'] == 'case_comparison']
        result['case_conditions_comparable'] = bool(eligible) and len({diagnostics[r['run_id']]['comparison_fingerprint'] for r in eligible}) == 1
    result['limitations'] = ['Expected bottleneck must be observed; low-work precheck is a separate condition',
        'VALID+UNSTABLE preserved; UNKNOWN Smoke cannot prove sustainable performance improvement',
        'Processing skew is not incoming skew when backlog/backpressure censors traffic',
        'Sampled synchronous operator-body CPU is approximate cost per sampled record, not subtask CPU usage']
    return result


def execute(config, experiment, output_dir, base='http://localhost:8081', runner=run, diagnostician=phase4_diagnose):
    from pathlib import Path
    if experiment != config['experiment'] or experiment not in [3,4]: raise ValueError('Wrong Phase 4 experiment')
    config = copy.deepcopy(config); plan = config['plan']; mode = config['mode']
    session_id = 'session_' + uuid.uuid4().hex
    directory = Path(output_dir) / ('experiment_' + str(experiment)) / session_id
    store = RunStore(directory); emit = store.jsonl('runs.jsonl')
    store.json('session.json', {'schema_version': 1, 'session_id': session_id, 'experiment': experiment,
         'mode': mode, 'started_wall_ms': time.time_ns()//1000000, 'frozen_config': config, 'cost_plan': cost_plan(config,experiment)})
    rows = []; diagnostics = {}; errors = []; precheck = None
    rng = random.Random(plan['order_seed'])
    def perform(case, ps, distribution, repeat, low_work=False):
        cfg = copy.deepcopy(config['run_config']); cfg['operator_parallelism'] = ps.copy()
        cfg['distribution'] = distribution; cfg['seed'] += repeat
        if low_work: cfg.update(cpu_work_iterations=0, keyed_cpu_work_iterations=0)
        context = {'session_id': session_id, 'phase': 4, 'case': case,
            'role': 'low_work_precheck' if low_work else 'case_comparison' if experiment == 3 else 'distribution_comparison',
            'parallelism': ps['heavy'] if experiment == 3 else ps['aggregation'], 'operator_parallelism': ps,
            'distribution': distribution, 'target_rate': cfg['target_rate'], 'repetition': repeat}
        print(json.dumps({'state':'RUN_START', **context}), flush=True)
        result = runner(cfg,mode,base,directory/'runs',experiment_id='experiment_'+str(experiment),context=context)
        row = {**result, **context, 'mode':mode, 'run_directory':os.path.relpath(result['result_directory'],directory)}
        rows.append(row); emit(row)
        diagnostics[row['run_id']] = diagnostician(result['result_directory'],config['evidence_thresholds'])
        print(json.dumps({'state':'RUN_END','run_id':row['run_id'],'validity':row['execution_validity'],'stability':row['performance_stability']}),flush=True)
        if row.get('cleanup_errors'): raise RuntimeError('Cleanup failed; refusing next run: '+str(row['cleanup_errors']))
        if any('KeyboardInterrupt' in e for e in row.get('observations',{}).get('execution_errors',[])): raise KeyboardInterrupt()
        return row
    try:
        if experiment == 3:
            cases = config['workload']['cases']
            controls = [perform('focused_transport_precheck',cases['bottleneck_focused'],'uniform',r,True) for r in range(plan['repetitions'])]
            precheck = precheck_decision(controls,diagnostics,config); store.json('precheck_decision.json',precheck)
            order = [(name,r) for name in cases for r in range(plan['repetitions'])]
            if plan['random_order']: rng.shuffle(order)
            for name,r in order: perform(name,cases[name],'uniform',r)
        else:
            order = [(p,d,r) for p in plan['parallelisms'] for d in plan['distributions'] for r in range(plan['repetitions'])]
            if plan['random_order']: rng.shuffle(order)
            for p,d,r in order: perform(d,{op:p for op in config['run_config']['operator_parallelism']},d,r)
    except (Exception,KeyboardInterrupt) as exc:
        errors.append(repr(exc))
    finally:
        status = 'ERROR' if errors else 'COMPLETED_WITH_RUN_FAILURES' if any(r['execution_validity'] in ['INVALID','ERROR'] for r in rows) else 'COMPLETED'
        comparison = comparison_summary([r for r in rows if r['run_id'] in diagnostics],diagnostics,config,precheck)
        summary = {'session_id':session_id,'experiment':experiment,'mode':mode,'status':status,
            'run_count':len(rows),'run_ids':[r['run_id'] for r in rows],'comparison':comparison,'errors':errors,
            'limitations':comparison['limitations']}
        table = [flatten(r,diagnostics.get(r['run_id'],{})) | {'case':r['case'],'distribution':r['distribution'],
                 'operator_parallelism':json.dumps(r['operator_parallelism'],sort_keys=True)} for r in rows]
        write_csv(directory/'summary.csv',table,list(table[0]) if table else ['run_id','case'])
        store.json('diagnostics.json',diagnostics); store.json('session_summary.json',summary); store.close()
    print(json.dumps({'session_directory':str(directory),'status':status,'runs':len(rows)}),flush=True)
    return directory,summary
