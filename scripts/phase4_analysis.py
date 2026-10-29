"""Measured experiment 3/4 tables, heatmaps and Korean reports."""
import json
import hashlib
import uuid
from pathlib import Path
from analyze_results import flatten, write_csv, METRICS
from capacity_analysis import stats, measured_samples
from phase4_diagnostics import resource_efficiency
from result_store import read_jsonl
from lab import save_json


def records_for_analysis(rows, diagnostics, comparison, experiment):
    paired_ids = {p[k] for p in comparison['pairs'] if p['status']=='VALID' for k in ['uniform_run_id','skewed_run_id']}
    records = []
    evidence={c['run_id']:c['expected_bottleneck_observed'] for c in comparison['cases']}
    for r in rows:
        d = diagnostics.get(r['run_id'],{}); resource = d.get('resource_efficiency',{})
        base = flatten(r,d); input_check = d.get('input_validation',{})
        load = d.get('aggregation_load',{}); candidate = d.get('bottleneck_candidate')
        base.update(case=r['case'],distribution=r['distribution'],repetition=r['repetition'],
            operator_parallelism=json.dumps(r['operator_parallelism'],sort_keys=True),
            condition_fingerprint=d.get('comparison_fingerprint'),
            input_comparison_validity=input_check.get('status'),
            eligible_for_paired_comparison=r['run_id'] in paired_ids if experiment==4 else comparison.get('case_conditions_comparable',False) and r['role']=='case_comparison',
            expected_bottleneck_observed=evidence.get(r['run_id'],False),
            generated_hot_ratio=input_check.get('actual_hot_key_ratio'),processed_hot_ratio=load.get('processed_hot_ratio'),
            hot_subtasks=json.dumps(load.get('hot_subtasks',[])),hot_placement_status=load.get('placement_status'),
            cohort_counts_reconciled=load.get('cohort_counts_reconciled'),
            aggregation_processed_max_mean=load.get('processed_cohort',{}).get('max_mean_ratio'),
            aggregation_processed_cv=load.get('processed_cohort',{}).get('cv'),
            aggregation_rate_max_mean=load.get('observed_output_rate',{}).get('max_mean_ratio'),
            aggregation_rate_cv=load.get('observed_output_rate',{}).get('cv'),
            tm_observed_core_seconds=resource.get('taskmanager_observed_core_seconds'),
            tm_observed_interval_seconds=resource.get('observed_interval_seconds'),
            tm_observed_mean_cpu_cores=resource.get('taskmanager_observed_mean_cpu_cores'),
            tm_memory_bytes_mean=resource.get('memory_bytes',{}).get('mean'),
            actual_occupied_slots_max=resource.get('occupied_slots',{}).get('max'))
        o = r.get('observations',{})
        base.update(measurement_generated_records=o.get('measurement_generated_records'),
            measurement_completed_in_window=o.get('measurement_cohort_completed_within_window'),
            measurement_drain_completed=o.get('measurement_cohort_drain_completed'),
            measurement_unprocessed=o.get('measurement_cohort_unprocessed'))
        records.append(base)
    return records


EXTRA_METRICS = ['generated_hot_ratio','processed_hot_ratio','aggregation_processed_max_mean','aggregation_processed_cv',
    'aggregation_rate_max_mean','aggregation_rate_cv','tm_observed_core_seconds','tm_observed_interval_seconds',
    'tm_memory_bytes_mean','actual_occupied_slots_max','tm_observed_mean_cpu_cores']


def repeat_statistics(records):
    import pandas as pd
    frame = pd.DataFrame(records); output=[]
    if frame.empty: return output
    keys=['case','distribution','parallelism','operator_parallelism','target_rate','mode','condition_fingerprint']
    for values,group in frame.groupby(keys,dropna=False,sort=True):
        valid=group[(group.execution_validity=='VALID') & (group.input_comparison_validity=='VALID')]
        for name in METRICS+EXTRA_METRICS:
            vals=pd.to_numeric(valid[name],errors='coerce').dropna()
            output.append(dict(zip(keys,[None if pd.isna(v) else v for v in values])) | {'metric':name,
                'total_runs':len(group),'valid_runs':len(valid),'observed_runs':len(vals),
                'stable_runs':sum(valid.performance_stability=='STABLE'),
                'mean':float(vals.mean()) if len(vals) else None,'std':float(vals.std(ddof=1)) if len(vals)>1 else None,
                'min':float(vals.min()) if len(vals) else None,'max':float(vals.max()) if len(vals) else None})
    return output


def make_charts(out, records, diagnostics, experiment, mode):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    statuses={}
    valid=[r for r in records if r['execution_validity']=='VALID' and r['input_comparison_validity']=='VALID' and r['role']!='low_work_precheck']
    eligible=[r for r in valid if r['eligible_for_paired_comparison']]
    # Mixed budgets/artifacts must not make a shared comparison curve.
    if len({r['condition_fingerprint'] for r in eligible})!=1: eligible=[]
    def absent(name,why='No eligible measured values'): statuses[name]={'status':'Not Executed','reason':why}
    def save(fig,name,note):
        fig.tight_layout(); fig.savefig(out/name,dpi=150); plt.close(fig)
        statuses[name]={'status':'Generated','note':note}
    def comparison_plot(name,metrics,ylabel):
        points={}
        for r in eligible:
            label=r['case'] if experiment==3 else r['distribution']
            x=r['case'] if experiment==3 else r['parallelism']
            for field,legend in metrics.items():
                if r[field] is not None: points.setdefault((label,legend,x),[]).append(r[field])
        if not points: absent(name,'Missing observed values or incomplete/invalid comparison premises'); return
        fig,ax=plt.subplots(figsize=(8,4))
        if experiment==3:
            labels=list(dict.fromkeys(r['case'] for r in eligible)); width=.8/len(metrics)
            for j,(field,legend) in enumerate(metrics.items()):
                means=[]; errors=[]
                for label in labels:
                    s=stats(points.get((label,legend,label),[])); means.append(s['mean'] if s['mean'] is not None else np.nan); errors.append(s['std'] if s['std'] is not None else np.nan)
                xs=np.arange(len(labels))+j*width
                ax.bar(xs,means,width,label=legend)
                known=[(x,m,e) for x,m,e in zip(xs,means,errors) if np.isfinite(e)]
                for x,m,e in known: ax.errorbar(x,m,yerr=e,color='black',capsize=3)
            ax.set_xticks(np.arange(len(labels))+(len(metrics)-1)*width/2,labels)
        else:
            for label in ['uniform','skewed']:
                for legend in metrics.values():
                    series=sorted((x,stats(v)) for (l,g,x),v in points.items() if l==label and g==legend)
                    if series:
                        ax.plot([x for x,_ in series],[s['mean'] for _,s in series],marker='o',label=label+'/'+legend)
                        for x,s in series:
                            if s['std'] is not None: ax.errorbar(x,s['mean'],yerr=s['std'],color='gray',capsize=3)
            ax.set_xlabel('Aggregation parallelism')
        ax.set(ylabel=ylabel,title=name.removesuffix('.png')+' ['+mode+']'); ax.legend(); ax.grid(alpha=.2)
        save(fig,name,'Observed valid comparisons; one repeat has unknown SD. Run quantile means are not merged quantiles.')
    comparison_plot('throughput.png',{'sink_throughput':'Sink in measurement','actual_produced_rate':'Actual produced'},'records/sec')
    comparison_plot('latency.png',{'latency_p95_ms':'Run p95 mean','latency_p99_ms':'Run p99 mean'},'ms, histogram upper bounds')
    comparison_plot('cpu_core_seconds.png',{'tm_observed_core_seconds':'TM observed core-seconds'},'core-seconds, sampled measurement subset')
    comparison_plot('cpu_mean_cores.png',{'tm_observed_mean_cpu_cores':'TM observed mean CPU cores'},'mean used CPU cores, cgroup interval')
    comparison_plot('memory.png',{'tm_memory_bytes_mean':'TM memory'},'bytes')
    comparison_plot('slot_usage.png',{'actual_occupied_slots_max':'Actual occupied slots'},'slots (not sum of operator P)')
    def heatmap(name,operator,field,ylabel):
        data=[]; labels=[]
        width=max((diagnostics[r['run_id']]['config']['operator_parallelism'][operator] for r in valid),default=0)
        for r in valid:
            vals=[None]*width
            for d in diagnostics[r['run_id']]['subtask_details']:
                if d['operator']==operator: vals[d['subtask']]=d[field]
            actual_p=diagnostics[r['run_id']]['config']['operator_parallelism'][operator]
            data.append([np.nan if v is None else v for v in vals]); labels.append(f"{r['case']} P{actual_p} r{r['repetition']} {r['run_id'][-5:]}")
        if not data or not np.isfinite(np.asarray(data)).any(): absent(name); return
        fig,ax=plt.subplots(figsize=(9,max(3,len(labels)*.38))); masked=np.ma.masked_invalid(np.asarray(data))
        scale={'vmin':0,'vmax':1000} if field in ['busy_ms_per_second','idle_ms_per_second','backpressure_ms_per_second'] else {}
        im=ax.imshow(masked,aspect='auto',interpolation='nearest',**scale); fig.colorbar(im,ax=ax,label=ylabel)
        ax.set_xticks(range(width)); ax.set_yticks(range(len(labels)),labels); ax.set(xlabel='Actual subtask index',title=operator+' '+field+' ['+mode+']')
        save(fig,name,'Individual measured runs; masked cells are Missing/not configured, actual zero counts remain zero.')
    heatmap('subtask_throughput.png','aggregation','output_rate','observed output records/sec')
    heatmap('subtask_busy.png','aggregation','busy_ms_per_second','busy ms/sec (not CPU)')
    heatmap('subtask_cpu_sample.png','aggregation','sampled_body_cpu_ns_per_record','sampled synchronous body CPU ns/record (approximate)')
    heatmap('source_throughput.png','source','output_rate','observed source output records/sec')
    if experiment==3:
        for field,name in [('busy_ms_per_second','operator_busy.png'),('idle_ms_per_second','operator_idle.png'),('backpressure_ms_per_second','operator_backpressure.png'),('observed_output_rate','operator_throughput.png')]:
            matrix=[[diagnostics[r['run_id']]['operators'][op].get(field) for op in ['source','parse','heavy','aggregation','sink']] for r in valid]
            if not matrix or not any(v is not None for line in matrix for v in line): absent(name); continue
            fig,ax=plt.subplots(figsize=(8,max(3,len(matrix)*.38)))
            scale={'vmin':0,'vmax':1000} if field!='observed_output_rate' else {}
            im=ax.imshow(np.ma.masked_invalid(np.array([[np.nan if v is None else v for v in line] for line in matrix])),aspect='auto',**scale)
            fig.colorbar(im,ax=ax,label='records/sec' if field=='observed_output_rate' else 'ms/sec, operator mean (not CPU)')
            ax.set_xticks(range(5),['source','parse','heavy','aggregation','sink']); ax.set_yticks(range(len(valid)),[r['case']+' r'+str(r['repetition']) for r in valid]); ax.set_title(field+' ['+mode+']'); save(fig,name,'Observed operator means; max-subtask evidence remains in diagnostics JSON/CSV.')
    else:
        points=[r for r in eligible if r['aggregation_rate_max_mean'] is not None and r['sink_throughput'] is not None]
        if points:
            fig,ax=plt.subplots(figsize=(7,4))
            for d in ['uniform','skewed']:
                rr=[r for r in points if r['distribution']==d]; ax.scatter([r['aggregation_rate_max_mean'] for r in rr],[r['sink_throughput'] for r in rr],label=d)
            ax.set(xlabel='Max/mean observed subtask output rate',ylabel='Sink records/sec',title='Observed imbalance (not incoming load) ['+mode+']'); ax.legend(); save(fig,'imbalance_vs_throughput.png','Correlation only; backlog/backpressure may censor incoming skew.')
        else: absent('imbalance_vs_throughput.png')
        hot=[]
        for r in eligible:
            if r['distribution']!='skewed' or r['hot_placement_status']!='OBSERVED_SINGLE_SUBTASK': continue
            d=diagnostics[r['run_id']]; indices=d['aggregation_load']['hot_subtasks']
            body=next(x for x in d['subtask_details'] if x['operator']=='aggregation' and x['subtask']==indices[0])
            duration=d['config']['measurement_seconds']
            if body['measurement_window_cohort_processed'] is not None and body['measurement_window_hot_processed'] is not None:
                hot.append((r['parallelism'],body['measurement_window_cohort_processed']/duration,body['measurement_window_hot_processed']/duration))
        if hot:
            fig,ax=plt.subplots(figsize=(7,4))
            for col,label in [(1,'Observed hot-subtask total cohort'),(2,'Observed hot-key cohort')]:
                ps=sorted(set(p for p,_,_ in hot)); vals=[stats([v[col] for v in hot if v[0]==p]) for p in ps]
                ax.plot(ps,[s['mean'] for s in vals],marker='o',label=label)
            ax.set(xlabel='Aggregation parallelism',ylabel='records/sec within measurement wall window',title='Observed hot-subtask load ['+mode+']'); ax.legend(); save(fig,'hot_subtask_load.png','Placement observed by actual measurement counters; Drain excluded. No predicted assignment rendered.')
        else: absent('hot_subtask_load.png','No reconciled observed single hot subtask in valid pairs')
        partitions=[diagnostics[r['run_id']]['input_validation'].get('partition_counts',[]) for r in valid]
        if partitions and len({len(v) for v in partitions})==1 and all(v for v in partitions):
            fig,ax=plt.subplots(figsize=(10,max(3,len(valid)*.38))); im=ax.imshow(partitions,aspect='auto'); fig.colorbar(im,ax=ax,label='Measurement ACK records'); ax.set(xlabel='Kafka partition',title='Actual partition distribution ['+mode+']'); ax.set_yticks(range(len(valid)),[r['case']+' P'+str(r['parallelism'])+' r'+str(r['repetition']) for r in valid]); save(fig,'partition_distribution.png','Actual producer partition ACK counts, independent of userId.')
        else: absent('partition_distribution.png')
    return statuses


def report(out, cfg, summary, records, diagnostics, statuses):
    experiment=summary['experiment']; comparison=summary['comparison']
    question='모든 Operator를 늘리는 방법과 병목 Operator 중심 증설은 어떻게 다른가?' if experiment==3 else 'Parallelism 부족과 Hot Key 집중을 어떤 관측으로 구별할 수 있는가?'
    lines=[f'# 실험 {experiment} 분석 ({summary["mode"]})','','## 1. 실험 질문','',question,'',
        '## 2. 가설','','실험3에서는 Heavy 중심 증설이 개선을 보일 수 있고, 실험4에서는 한 Key의 작업이 한 Subtask에 남을 수 있다. 가설은 실측 결과가 아니다. 의도한 병목이 실제 관측됐는지 별도로 판정한다.','',
        '## 3. 실험 환경','','고정 물리 자원·Chaining off. 전체 frozen 설정과 Run별 이미지/CPU quota/메모리/Clock/Graph는 session.json 및 environments.json, metadata.json에 보존한다.','',
        '```json',json.dumps(cfg['frozen_config'],ensure_ascii=False,indent=2),'```','',
        '## 4. 측정 지표','','Producer Target/Sent/ACK, 측정 코호트 시간 내 완료/Drain, Lag 기울기/CI, Latency 샘플/완료율, 실제 Slot·TM CPU core-seconds/메모리, Operator 및 Subtask Busy/Idle/Backpressure/처리율을 사용한다. Hot Key 담당 Subtask는 실제 Aggregation Counter/Gauge로 확인한다. CPU Time은 샘플링된 동기 연산 구간 비용이며 전체 Subtask CPU 사용률이 아니다.','',
        '## 5. 실험 결과','',f'자동화 상태: {summary["status"]}. 개별 Run {len(records)}개. 실패·저성능 Run도 보존한다.','',
        '| Run | Case/분포 | 실제 Operator P | 유효성/안정성 | Sink records/sec | p99 ms | 생성 Hot 비율 | 처리 부하 Max/Mean |',
        '|---|---|---|---|---:|---:|---:|---:|']
    def v(value): return 'Missing' if value is None else f'{value:.4g}' if isinstance(value,float) else str(value)
    for r in records:
        lines.append('| '+' | '.join([r['run_id'],r['case']+'/'+r['distribution'],r['operator_parallelism'],r['execution_validity']+'/'+r['performance_stability'],v(r['sink_throughput']),v(r['latency_p99_ms']),v(r['generated_hot_ratio']),v(r['aggregation_processed_max_mean'])])+' |')
    lines += ['', '구간별 건수·Latency 샘플 수/코호트 완료율은 summary.csv, 생성/처리 Hot 비율 및 실제 Subtask Count·Rate·CPU 샘플은 subtask_diagnostics.csv/diagnostics.json을 따른다. Drain 처리량은 Measurement Throughput에 포함하지 않았다. 반복 통계의 1회 Std는 Missing이며 Run별 p99 평균은 전체 이벤트 p99가 아니다.','',
        '비교 전제·실제 병목 후보·Hot 배치 검증:','','```json',json.dumps(comparison,ensure_ascii=False,indent=2),'```']
    for name,status in statuses.items(): lines += ['',f'![{name}]({name})' if status['status']=='Generated' else name+': **Not Executed** — '+status['reason']]
    lines += ['','## 6. 원인 분석','',
        '병목 후보는 Subtask 최대 Busy와 Downstream Backpressure, 다른 Operator와의 차이로 찾는다. 높은 Upstream Backpressure 하나만으로 원인을 확정하지 않는다. Hot Key가 한 Subtask에 관측된 것과 Aggregation이 성능 병목이라는 주장은 별개다. expected_bottleneck_observed=false이면 의도한 병목 실험의 해석은 보류한다. Partition 비교가 실패하면 Uniform/Skewed 공통 성능 곡선을 생성하지 않는다. 저작업량 사전 점검의 성공도 실제 Focused Case의 선포화를 배제하지 못하므로 실제 메트릭을 함께 확인한다.','',
        '## 7. 실무 적용 기준','','Operator 입력률·실측 병목과 물리 CPU 예산을 확인한 후 병렬도를 정한다. 동일 처리율에서도 Latency·Checkpoint·Headroom과 Idle/Slot/메모리 비용을 함께 비교한다. 단일 Hot Key가 한 Subtask에 남는다면 P 증가만으로 해당 Key를 분할하지 못한다. 선택적 Salting/Two-stage는 이번에 구현하지 않았고, 매 입력의 최신 누적값과 출력 순서를 원래와 같게 유지하기 어려울 수 있다.','',
        '## 8. 실험의 한계','','짧은 Smoke는 지속 Capacity나 개선 효과의 통계적 증명이 아니다. Full 반복/고부하·계측 CPU 오버헤드 on/off 정량 비교는 실제 기록 없으면 Not Executed다. 고정 CPU에서 P 증가만으로 코어가 늘지 않는다. 처리된 부하는 Backpressure로 검열될 수 있어 생성 Key 분포와 구별한다. REST 비원자성·신선도·Clock 오차·log2 Histogram·JIT/State 생성·외부 I/O 없는 Sink·CPU 샘플의 대표성/계측 비용을 고려한다.']
    if summary['mode']=='smoke': lines += ['','**Sustainable Capacity 및 장시간 성능·병목 가설 검증: Not Executed.** UNKNOWN을 STABLE로 바꾸지 않았다.']
    (out/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')


def analyze(session, experiment):
    import pandas as pd
    import matplotlib
    import numpy as np
    session=Path(session).resolve(); cfg=json.loads((session/'session.json').read_text()); summary=json.loads((session/'session_summary.json').read_text())
    if cfg['experiment']!=experiment or summary['experiment']!=experiment: raise ValueError('Wrong experiment')
    rows=read_jsonl(session/'runs.jsonl'); diagnostics=json.loads((session/'diagnostics.json').read_text())
    if len({r['run_id'] for r in rows})!=len(rows) or any(r['mode']!=summary['mode'] for r in rows): raise ValueError('Duplicate run or mixed modes')
    # Recompute derived resource cost from immutable raw data, respecting cgroup timestamp bounds.
    trends=[]
    for r in rows:
        directory=session/r['run_directory']; m=json.loads((directory/'metadata.json').read_text())
        samples=measured_samples(directory,m)
        if r['run_id'] in diagnostics: diagnostics[r['run_id']]['resource_efficiency']=resource_efficiency(samples,r.get('observations',{}),m.get('plan'))
        for sample in samples:
            interval=sample.get('latency_interval',{})
            trends.append({'run_id':r['run_id'],'case':r['case'],'distribution':r['distribution'],'parallelism':r['parallelism'],
                'seconds_since_measurement':(sample['wall_time_ms']-m['plan']['measurement_start_wall_ms'])/1000,
                'interval_p95_ms':interval.get('p95'),'interval_p99_ms':interval.get('p99'),'sample_count':interval.get('sample_count')})
    records=records_for_analysis(rows,diagnostics,summary['comparison'],experiment)
    out=session/('analysis_'+uuid.uuid4().hex); out.mkdir(exist_ok=False)
    write_csv(out/'summary.csv',records,list(records[0]) if records else ['run_id','case'])
    repeats=repeat_statistics(records); write_csv(out/'repeat_statistics.csv',repeats,list(repeats[0]) if repeats else ['case','metric'])
    details=[{'run_id':r['run_id'],'case':r['case'],'distribution':r['distribution'],'execution_validity':r['execution_validity'],**x} for r in rows for x in diagnostics.get(r['run_id'],{}).get('subtask_details',[])]
    write_csv(out/'subtask_diagnostics.csv',details,list(details[0]) if details else ['run_id','operator','subtask'])
    operators=[{'run_id':r['run_id'],'case':r['case'],'operator':op,**v} for r in rows for op,v in diagnostics.get(r['run_id'],{}).get('operators',{}).items()]
    write_csv(out/'operator_diagnostics.csv',operators,list(operators[0]) if operators else ['run_id','operator'])
    save_json(out/'comparisons.json',summary['comparison'])
    save_json(out/'diagnostics.json',diagnostics)
    environments=[]
    for r in rows:
        m=json.loads((session/r['run_directory']/'metadata.json').read_text()); env=m.get('environment',{})
        environments.append({'run_id':r['run_id'],'host':env.get('host'),'flink':env.get('flink_overview'),
            'config':m['config'],'artifact_hashes':m.get('artifact_hashes'),'clock_probes':env.get('clock_probes'),
            'containers':[{'service':c.get('Config',{}).get('Labels',{}).get('com.docker.compose.service'),'image':c.get('Image'),'limits':c.get('HostConfig')} for c in env.get('docker_inspect',[])]})
    save_json(out/'environments.json',environments)
    write_csv(out/'latency_trend.csv',trends,list(trends[0]) if trends else ['run_id','seconds_since_measurement','interval_p99_ms'])
    statuses=make_charts(out,records,diagnostics,experiment,summary['mode'])
    if any(t['interval_p99_ms'] is not None for t in trends):
        import matplotlib.pyplot as plt
        fig,ax=plt.subplots(figsize=(8,4))
        for rid in dict.fromkeys(t['run_id'] for t in trends):
            observed=[t for t in trends if t['run_id']==rid and t['interval_p99_ms'] is not None]
            if observed: ax.plot([t['seconds_since_measurement'] for t in observed],[t['interval_p99_ms'] for t in observed],marker='.',label=observed[0]['case']+' '+rid[-5:])
        ax.set(xlabel='Seconds since measurement',ylabel='Interval p99 ms, histogram upper bounds',title='Latency interval observations ['+summary['mode']+']'); ax.legend()
        fig.tight_layout(); fig.savefig(out/'latency_trend.png',dpi=150); plt.close(fig)
        statuses['latency_trend.png']={'status':'Generated','note':'Individual sampled cohort interval quantiles; no averaging subtask quantiles'}
    else: statuses['latency_trend.png']={'status':'Not Executed','reason':'No observed interval latency'}
    report(out,cfg,summary,records,diagnostics,statuses)
    hashes={str(p.relative_to(Path(__file__).resolve().parents[1])):hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('*.py')}
    save_json(out/'analysis.json',{'source_session':str(session),'versions':{'pandas':pd.__version__,'matplotlib':matplotlib.__version__,'numpy':np.__version__},'analyzer_artifact_hashes':hashes,
        'resource_cost_recomputed_from_raw':True,'charts':statuses,'valid_runs':sum(r['execution_validity']=='VALID' for r in rows)})
    print(json.dumps({'analysis_directory':str(out),'charts':statuses},ensure_ascii=False,indent=2)); return out
