"""Isolated experiment 5 source-scaling runs using the existing Phase 2 lifecycle."""
import copy
import json
import os
import random
import time
import uuid
from pathlib import Path
from run_once import run
from result_store import RunStore
from analyze_results import flatten, write_csv
from phase5_config import cost_plan
from phase5_diagnostics import phase5_diagnose


def execute(config, experiment, output_dir, base='http://localhost:8081', runner=run, diagnostician=phase5_diagnose):
    if experiment!=5 or config['experiment']!=5: raise ValueError('Wrong experiment')
    config=copy.deepcopy(config); plan=config['plan']; mode=config['mode']
    session_id='session_'+uuid.uuid4().hex; directory=Path(output_dir)/'experiment_5'/session_id
    store=RunStore(directory); emit=store.jsonl('runs.jsonl'); rows=[]; diagnostics={}; errors=[]
    store.json('session.json',{'schema_version':1,'session_id':session_id,'experiment':5,'mode':mode,
        'started_wall_ms':time.time_ns()//1000000,'frozen_config':config,'cost_plan':cost_plan(config)})
    order=[(p,r) for p in plan['parallelisms'] for r in range(plan['repetitions'])]
    if plan['random_order']: random.Random(plan['order_seed']).shuffle(order)
    try:
        for p,repeat in order:
            cfg=copy.deepcopy(config['run_config']); cfg['operator_parallelism']['source']=p; cfg['seed']+=repeat
            context={'session_id':session_id,'phase':5,'role':'source_comparison','case':'source_p'+str(p),
                'parallelism':p,'operator_parallelism':cfg['operator_parallelism'].copy(),'distribution':'uniform',
                'target_rate':cfg['target_rate'],'repetition':repeat}
            print(json.dumps({'state':'RUN_START',**context}),flush=True)
            result=runner(cfg,mode,base,directory/'runs',experiment_id='experiment_5',context=context)
            row={**result,**context,'mode':mode,'run_directory':os.path.relpath(result['result_directory'],directory)}
            rows.append(row); emit(row); diagnostics[row['run_id']]=diagnostician(result['result_directory'],config)
            print(json.dumps({'state':'RUN_END','run_id':row['run_id'],'validity':row['execution_validity'],
                'assignment':diagnostics[row['run_id']]['source_assignment']['status']}),flush=True)
            if row.get('cleanup_errors'): raise RuntimeError('Cleanup failed; refusing next run: '+str(row['cleanup_errors']))
            if any('KeyboardInterrupt' in e for e in row.get('observations',{}).get('execution_errors',[])): raise KeyboardInterrupt()
    except (Exception,KeyboardInterrupt) as exc: errors.append(repr(exc))
    finally:
        status='ERROR' if errors else 'COMPLETED_WITH_RUN_FAILURES' if any(r['execution_validity'] in ['ERROR','INVALID'] for r in rows) else 'COMPLETED'
        comparable=len({d['comparison_fingerprint'] for d in diagnostics.values()})==1
        summary={'session_id':session_id,'experiment':5,'mode':mode,'status':status,'run_count':len(rows),
            'run_ids':[r['run_id'] for r in rows],'errors':errors,'conditions_comparable':comparable,
            'limitations':['Observed unassigned readers are not inferred from P minus partition count',
                'Throughput at a fixed low offered rate is not source capacity; downstream/resource premises are conditional',
                'No event-time watermarks; with event time, idle readers need a separate withIdleness validation']}
        table=[flatten(r,diagnostics.get(r['run_id'],{})) for r in rows]
        write_csv(directory/'summary.csv',table,list(table[0]) if table else ['run_id'])
        store.json('diagnostics.json',diagnostics); store.json('session_summary.json',summary); store.close()
    print(json.dumps({'session_directory':str(directory),'status':status,'runs':len(rows)}),flush=True)
    return directory,summary
