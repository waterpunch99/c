"""Actual addSplits gauges, independent read-offset witnesses and conservative premises."""
import json
import hashlib
import re
from pathlib import Path
from collect_metrics import metric, operator_key, source_offsets
from phase4_diagnostics import phase4_diagnose
from result_store import read_jsonl


def assignment_snapshot(sample, partitions, source_p, max_age_ms, restart_count=0):
    sources=[r for r in sample.get('subtasks',[]) if operator_key(r['operator'])=='source']
    rows=[]; owners={}; reasons=[]; invalid=False
    if len(sources)!=source_p or {r['subtask'] for r in sources}!=set(range(source_p)): reasons.append('Missing/duplicate source subtask inventory')
    for r in sources:
        ids=[]; metrics=r['metrics']; stamp=metric(metrics,'sourceAssignmentSnapshotTimestampMs')
        count=metric(metrics,'sourceAssignedPartitionCount')
        fresh=stamp is not None and 0<=sample['wall_time_ms']-stamp<=max_age_ms
        for name,value in metrics.items():
            match=re.search(r'(?:^|\.)sourceAssignedPartition(\d+)$',name)
            if match:
                p=int(match.group(1))
                if metric({name:value},name)!=1 or p not in range(partitions) or p in ids: invalid=True
                ids.append(p)
                owners.setdefault(p,[]).append(r['subtask'])
        complete=fresh and type(count) is int and count>=0 and count==len(ids)
        if not complete: reasons.append('Missing/stale/count-inconsistent assignment on subtask '+str(r['subtask']))
        rows.append({'source_subtask':r['subtask'],'assigned_partition_count':count if complete else None,
            'assigned_partitions':sorted(ids) if complete else None,'taskmanager_id':r.get('taskmanager_id'),
            'num_records_out':metric(metrics,'numRecordsOut'),'pending_records':metric(metrics,'pendingRecords'),
            'assignment_timestamp_ms':stamp,'evidence_complete':complete})
    if any(len(v)!=1 for v in owners.values()): invalid=True; reasons.append('Duplicate partition ownership')
    if set(owners)!=set(range(partitions)): reasons.append('Not all Kafka partitions observed in assignment callbacks')
    reads,conflicts=source_offsets(sources)
    if conflicts or any(p in owners and owners[p]!=[v['source_subtask']] for p,v in reads.items()): invalid=True; reasons.append('Assignment/read witness conflict')
    if restart_count!=0: reasons.append('Restart evidence missing/nonzero; assignment epoch uncertain')
    complete=not reasons and not invalid
    return {'status':'INVALID' if invalid else 'OBSERVED_COMPLETE' if complete else 'INCONCLUSIVE',
        'reasons':reasons,'subtasks':sorted(rows,key=lambda x:x['source_subtask']),
        'partition_owners':{str(p):v[0] for p,v in owners.items() if len(v)==1} if complete else {},
        'unassigned_subtasks':[r['source_subtask'] for r in rows if r['assigned_partition_count']==0] if complete else None,
        'method':'SourceReader.addSplits accepted callbacks + per-reader count incl zero; fixed single topic, discovery off, unbounded, no restart'}


def phase5_diagnose(directory, config):
    directory=Path(directory); d=phase4_diagnose(directory,config['evidence_thresholds'])
    metadata=json.loads((directory/'metadata.json').read_text()); cfg=metadata['config']
    downstream_graph={op:p for op,p in cfg['operator_parallelism'].items() if op!='source'}
    d['comparison_fingerprint']=hashlib.sha256(json.dumps({'base':d['comparison_fingerprint'],'fixed_downstream':downstream_graph},sort_keys=True).encode()).hexdigest()
    summary=json.loads((directory/'summary.json').read_text()); observations=summary.get('observations',{})
    samples=read_jsonl(directory/'raw_samples.jsonl')
    assignments=[{'wall_time_ms':s['wall_time_ms'],**assignment_snapshot(s,cfg['kafka_partitions'],cfg['operator_parallelism']['source'],
        cfg['thresholds']['maximum_metric_age_seconds']*1000,observations.get('restart_count'))} for s in samples]
    final=assignments[-1] if assignments else {'status':'INCONCLUSIVE','reasons':['No raw samples'],'subtasks':[],'partition_owners':{},'unassigned_subtasks':None}
    complete=[a for a in assignments if a['status']=='OBSERVED_COMPLETE']
    changed=len({json.dumps(a['partition_owners'],sort_keys=True) for a in complete})>1
    if changed: final={**final,'status':'INVALID','reasons':final['reasons']+['Partition ownership changed within run']}
    downstream=[d['operator_max_busy'].get(op) for op in ['parse','heavy','aggregation','sink']]
    premises='INVALID' if d['input_validation']['status']=='INVALID' or final['status']=='INVALID' else 'INCONCLUSIVE'
    if summary['execution_validity']=='VALID' and d['input_validation']['status']=='VALID' and final['status']=='OBSERVED_COMPLETE' and all(v is not None and v<config['maximum_downstream_busy_ms_per_second'] for v in downstream) and d['bottleneck_candidate'] not in ['parse','heavy','aggregation','sink']:
        premises='VALID'
    d.update(source_assignment=final,assignment_timeline=assignments,
        source_comparison_premises={'status':premises,'downstream_max_busy_ms_per_second':dict(zip(['parse','heavy','aggregation','sink'],downstream)),
            'applied_maximum_downstream_busy':config['maximum_downstream_busy_ms_per_second'],
            'source_saturation_observed':d['bottleneck_candidate']=='source',
            'reason':'Registered input/assignment/low downstream busy predicates; low-load success does not prove downstream headroom at other rates'})
    return d
