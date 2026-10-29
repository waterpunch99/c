"""Source assignment and measured source scaling, CSV/JSON/PNG/Korean report."""
import hashlib
import json
import uuid
from pathlib import Path
from analyze_results import flatten, write_csv, METRICS
from capacity_analysis import stats, measured_samples
from collect_metrics import metric, operator_key
from result_store import read_jsonl
from lab import save_json


def records_for_analysis(rows, diagnostics):
    records=[]
    for r in rows:
        d=diagnostics.get(r['run_id'],{}); assignment=d.get('source_assignment',{}); premises=d.get('source_comparison_premises',{})
        source=d.get('operators',{}).get('source',{})
        records.append(flatten(r,d) | {'repetition':r['repetition'],
            'operator_parallelism':json.dumps(r['operator_parallelism'],sort_keys=True),
            'condition_fingerprint':d.get('comparison_fingerprint'),
            'source_throughput':source.get('observed_output_rate'),
            'source_busy':source.get('busy_ms_per_second'),'source_idle':source.get('idle_ms_per_second'),
            'source_backpressure':source.get('backpressure_ms_per_second'),
            'assignment_status':assignment.get('status'),'unassigned_source_subtasks':json.dumps(assignment.get('unassigned_subtasks')),
            'comparison_premises':premises.get('status'),'source_saturation_observed':premises.get('source_saturation_observed')})
    return records


def repeat_statistics(records):
    groups={}; output=[]
    for r in records:
        key=tuple(r[k] for k in ['parallelism','target_rate','mode','condition_fingerprint'])
        groups.setdefault(key,[]).append(r)
    for key,group in sorted(groups.items(),key=lambda item:str(item[0])):
        valid=[r for r in group if r['execution_validity']=='VALID' and r['comparison_premises']=='VALID']
        for name in METRICS+['source_throughput','source_busy','source_idle','source_backpressure']:
            values=[r[name] for r in valid if r.get(name) is not None]
            output.append(dict(zip(['parallelism','target_rate','mode','condition_fingerprint'],key)) | {'metric':name,
                'total_runs':len(group),'valid_runs':len(valid),'observed_runs':len(values),
                'stable_runs':sum(r['performance_stability']=='STABLE' for r in valid),**stats(values)})
    return output


def make_charts(out, records, diagnostics, timelines, mode):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    statuses={}; valid=[r for r in records if r['execution_validity']=='VALID' and r['comparison_premises']=='VALID']
    eligible=valid if len({r['condition_fingerprint'] for r in valid})==1 else []
    def save(fig,name):
        fig.tight_layout(); fig.savefig(out/name,dpi=150); plt.close(fig)
        statuses[name]={'status':'Generated','note':'Observed values only; individual run quantiles are not pooled quantiles; Smoke is not capacity'}
    def absent(name): statuses[name]={'status':'Not Executed','reason':'Missing/unsupported values or comparison premises not established'}
    for name,fields,ylabel in [('throughput.png',['source_throughput','sink_throughput','actual_produced_rate'],'records/sec'),
        ('latency.png',['latency_p95_ms','latency_p99_ms'],'ms, histogram upper bound'),
        ('source_timing.png',['source_busy','source_idle','source_backpressure'],'ms/sec (not CPU)'),
        ('backlog_slope.png',['backlog_slope'],'records/sec'),('cpu.png',['tm_cpu_quota_utilization'],'fraction of actual TM CPU quota')]:
        points={}
        for field in fields:
            for p in sorted({r['parallelism'] for r in eligible}):
                values=[r[field] for r in eligible if r['parallelism']==p and r.get(field) is not None]
                if values: points.setdefault(field,[]).append((p,stats(values)))
        if not points: absent(name); continue
        fig,ax=plt.subplots(figsize=(8,4))
        for field,series in points.items():
            ax.plot([p for p,s in series],[s['mean'] for p,s in series],marker='o',label=field)
            for p,s in series:
                if s['std'] is not None: ax.errorbar(p,s['mean'],yerr=s['std'],color='gray',capsize=3)
        ax.set(xlabel='Source parallelism (fixed downstream)',ylabel=ylabel,title=name.removesuffix('.png')+' ['+mode+']'); ax.legend(); ax.grid(alpha=.2); save(fig,name)
    assignment_records=[r for r in records if r['assignment_status']=='OBSERVED_COMPLETE']
    width=max((r['parallelism'] for r in valid+assignment_records),default=0)
    for name,field in [('source_subtask_throughput.png','output_rate'),('source_subtask_busy.png','busy_ms_per_second'),('source_subtask_idle.png','idle_ms_per_second'),('assigned_partition_count.png','assigned_partition_count')]:
        matrix=[]; labels=[]
        for r in assignment_records if field=='assigned_partition_count' else valid:
            d=diagnostics[r['run_id']]; row=[np.nan]*width
            details=d['source_assignment']['subtasks'] if field=='assigned_partition_count' else [s for s in d['subtask_details'] if s['operator']=='source']
            for s in details:
                i=s['source_subtask'] if field=='assigned_partition_count' else s['subtask']
                row[i]=s.get(field) if s.get(field) is not None else np.nan
            matrix.append(row); labels.append('P'+str(r['parallelism'])+' r'+str(r['repetition'])+' '+r['execution_validity']+' '+r['run_id'][-5:])
        if not matrix or not np.isfinite(matrix).any(): absent(name); continue
        fig,ax=plt.subplots(figsize=(10,max(3,len(labels)*.4)))
        scale={'vmin':0,'vmax':1000} if field.endswith('ms_per_second') else {}
        im=ax.imshow(np.ma.masked_invalid(np.array(matrix)),aspect='auto',**scale); fig.colorbar(im,ax=ax,label=field)
        ax.set_xticks(range(width)); ax.set_yticks(range(len(labels)),labels); ax.set(xlabel='Actual source subtask index',title=name.removesuffix('.png')+' ['+mode+']'); save(fig,name)
    for name,field,ylabel in [('backlog_trend.png','read_backlog','records, source next offsets'),('committed_lag_trend.png','committed_lag','records, checkpoint commit lag'),('pending_records.png','pending_records','connector pending records (not read backlog)'),('latency_trend.png','interval_p99_ms','sampled interval p99 ms')]:
        data=[t for t in timelines if t.get(field) is not None]
        if not data: absent(name); continue
        fig,ax=plt.subplots(figsize=(8,4))
        for rid in dict.fromkeys(t['run_id'] for t in data):
            series=[t for t in timelines if t['run_id']==rid]
            ax.plot([t['seconds_since_measurement'] for t in series],[t.get(field) if t.get(field) is not None else np.nan for t in series],marker='.',label='P'+str(series[0]['parallelism'])+' '+series[0].get('execution_validity','Missing')+' '+rid[-5:])
        ax.set(xlabel='Seconds since measurement',ylabel=ylabel,title=name.removesuffix('.png')+' ['+mode+']'); ax.legend(); save(fig,name)
    return statuses


def report(out, config, summary, records, diagnostics, statuses):
    def v(x): return 'Missing' if x is None else f'{x:.4g}' if isinstance(x,float) else str(x)
    lines=['# 실험 5: Kafka Partition과 Source Parallelism ('+summary['mode']+')','','## 1. 실험 질문','',
        'Partition 수와 Source P가 실제 할당·처리율·Idle에 어떤 영향을 주는가? Downstream과 다른 P를 적용할 수 있는가?','',
        '## 2. 가설','','Partition보다 많은 Source Reader에는 할당이 없는 Reader가 생길 수 있다. P 증가가 처리량 개선을 보장하지 않는다. 예상 배치를 측정값으로 사용하지 않는다.','',
        '## 3. 실험 환경','','Run별 metadata.json/environments.json의 실제 CPU quota·이미지·Slot·Graph·Clock과 고정 설정을 따른다.','',
        '```json',json.dumps(config['frozen_config'],ensure_ascii=False,indent=2),'```','',
        '## 4. 측정 지표','','실제 addSplits 콜백의 Partition ID·Count(0 포함), 읽기 Offset 증거, Source/Subtask 처리율·Busy/Idle/Backpressure, Producer ACK, Sink, Latency, Kafka read/committed Lag, pendingRecords, CPU·Checkpoint를 구분한다.','',
        '## 5. 실험 결과','',f'자동화: {summary["status"]}. 조건 비교 가능: {summary["conditions_comparable"]}.','',
        '| Run | Source P | 유효성/안정성 | Source records/s | Sink records/s | p99 ms | 할당 판정 | 실제 미할당 Subtask |',
        '|---|---:|---|---:|---:|---:|---|---|']
    for r in records: lines.append('| '+' | '.join([r['run_id'],str(r['parallelism']),r['execution_validity']+'/'+r['performance_stability'],v(r['source_throughput']),v(r['sink_throughput']),v(r['latency_p99_ms']),v(r['assignment_status']),r['unassigned_source_subtasks']])+' |')
    lines+=['','Source rate는 Measurement 안에 완전히 들어오는 REST 누적 Counter 차분의 관측 부분 구간이며 Sink rate는 Measurement 시간 내 완료된 Measurement 생성 코호트 건수를 전체 Measurement 기간으로 나눈 값이다. 측정 시간 내 전체 완료(다른 코호트 포함)는 원본 observations.measurement_window_total_completed에 별도로 남긴다. 서로 다른 구간과 코호트의 rate가 정확히 같다고 가정하지 않는다. Drain 완료는 측정 처리량에 합산하지 않았다. 반복 통계의 1회 Std는 Missing이며 코호트 완료율·Latency Sample Count는 summary.csv를 확인한다. Latency 샘플이 적은 Smoke의 p99는 통계적으로 불확실하다.','',
        '공통 성능 비교는 VALID Run과 비교 전제를 만족한 조건만 사용한다. 실제 할당 Count Heatmap은 독립적으로 OBSERVED_COMPLETE인 Run의 관측이며 INVALID도 상태를 표시해 보존한다. 개별 원시 시계열에도 Run 유효성을 표시하고 안정성 증거로 승격하지 않는다.','']
    for name,status in statuses.items(): lines += [f'![{name}]({name})' if status['status']=='Generated' else name+': **Not Executed** — '+status['reason'],'']
    lines+=['## 6. 원인 분석','', '실제 할당과 비교 전제:','','```json',json.dumps({rid:{'assignment':d['source_assignment'],'premises':d['source_comparison_premises']} for rid,d in diagnostics.items()},ensure_ascii=False,indent=2),'```','',
        '할당이 없다는 결론은 신선한 Count=0 Gauge와 모든 Partition의 유일한 소유자 관측이 있을 때만 내린다. 단순히 읽은 데이터가 없다는 이유로 미할당이라고 설명하지 않는다. Downstream Busy가 낮아도 다른 입력률의 여유 용량까지 검증한 것은 아니다. Source 포화가 관측되지 않으면 처리량 차이를 Source Capacity 한계라고 단정하지 않는다.','',
        '## 7. 실무 적용 기준','','Source P는 실제 Partition 할당과 Source 부하를 보고 결정한다. Keyed Operator P는 Key 분포와 해당 Operator Capacity를 별도로 확인한다. 고정 CPU 환경의 P 증가 비용을 Slot/CPU/Checkpoint·Latency와 함께 평가하고 Peak Rate에서 독립 검증한다.','',
        '## 8. 실험의 한계','','고정 단일 Topic, Partition Discovery off, Unbounded, Restore/Restart 없음에서만 콜백 누적 할당을 현재 할당으로 해석한다. Topic 변경·Split 종료·동적 Source·재시작은 이번 Adapter의 지원 범위 밖이다. pendingRecords는 Connector의 지연 등록 Gauge여서 미할당 Reader에서 Unsupported일 수 있으며 결측을 0으로 채우지 않는다. read Lag와 Commit Lag는 같은 값이 아니다. 기본 noWatermarks이므로 Event-time 지연은 측정하지 않았다. Event-time 도입 시 withIdleness와 Idle Source Watermark를 별도 검증해야 한다. 외부 I/O 없는 Sink·REST 비원자성·Clock·JIT·Checkpoint·Histogram 근사를 고려한다.','']
    if summary['mode']=='smoke': lines+=['**장시간 Source Capacity·최소 안정/운영 병렬도: Not Executed.** 짧은 Smoke UNKNOWN을 STABLE로 바꾸지 않았다.']
    (out/'report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')


def analyze(session, experiment=5):
    import pandas as pd
    import matplotlib
    import numpy as np
    session=Path(session).resolve(); config=json.loads((session/'session.json').read_text()); summary=json.loads((session/'session_summary.json').read_text())
    if experiment!=5 or config['experiment']!=5 or summary['experiment']!=5: raise ValueError('Wrong experiment')
    rows=read_jsonl(session/'runs.jsonl'); diagnostics=json.loads((session/'diagnostics.json').read_text())
    if len({r['run_id'] for r in rows})!=len(rows) or any(r['mode']!=summary['mode'] for r in rows): raise ValueError('Duplicate run/mixed modes')
    out=session/('analysis_'+uuid.uuid4().hex); out.mkdir(exist_ok=False)
    records=records_for_analysis(rows,diagnostics); repeats=repeat_statistics(records)
    write_csv(out/'summary.csv',records,list(records[0]) if records else ['run_id'])
    write_csv(out/'repeat_statistics.csv',repeats,list(repeats[0]) if repeats else ['metric'])
    assignment=[]; subtasks=[]; timelines=[]; environments=[]
    for r in rows:
        rid=r['run_id']; d=diagnostics.get(rid,{}); directory=session/r['run_directory']
        meta=json.loads((directory/'metadata.json').read_text()); environments.append({'run_id':rid,'config':meta['config'],'environment':meta.get('environment'),'artifact_hashes':meta.get('artifact_hashes')})
        for a in d.get('assignment_timeline',[]):
            for st in a['subtasks']:
                assignment.append({'run_id':rid,'parallelism':r['parallelism'],'wall_time_ms':a['wall_time_ms'],'assignment_status':a['status'],**st,'assigned_partitions':json.dumps(st['assigned_partitions'])})
        subtasks.extend({'run_id':rid,**st} for st in d.get('subtask_details',[]))
        for s in measured_samples(directory,meta):
            sources=[st for st in s['subtasks'] if operator_key(st['operator'])=='source']
            pending=[metric(st['metrics'],'pendingRecords') for st in sources]
            timelines.append({'run_id':rid,'parallelism':r['parallelism'],'execution_validity':r['execution_validity'],'performance_stability':r['performance_stability'],
                'seconds_since_measurement':(s['wall_time_ms']-meta['plan']['measurement_start_wall_ms'])/1000,
                'read_backlog':s.get('offsets',{}).get('read_backlog'),'committed_lag':s.get('offsets',{}).get('committed_lag'),
                'pending_records':sum(pending) if pending and all(v is not None and v>=0 for v in pending) else None,
                'pending_records_observed_subtasks':sum(v is not None and v>=0 for v in pending),
                'interval_p99_ms':s.get('latency_interval',{}).get('p99'),'interval_latency_samples':s.get('latency_interval',{}).get('sample_count')})
    for name,data,empty in [('assignments.csv',assignment,['run_id','source_subtask']),('subtask_diagnostics.csv',subtasks,['run_id','operator','subtask']),('metric_trends.csv',timelines,['run_id','seconds_since_measurement'])]:
        write_csv(out/name,data,list(data[0]) if data else empty)
    save_json(out/'diagnostics.json',diagnostics); save_json(out/'environments.json',environments)
    statuses=make_charts(out,records,diagnostics,timelines,summary['mode']); report(out,config,summary,records,diagnostics,statuses)
    hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('*.py')}
    save_json(out/'analysis.json',{'source_session':str(session),'versions':{'pandas':pd.__version__,'matplotlib':matplotlib.__version__,'numpy':np.__version__},
        'analyzer_artifact_hashes':hashes,'charts':statuses,'valid_runs':sum(r['execution_validity']=='VALID' for r in rows)})
    print(json.dumps({'analysis_directory':str(out),'charts':statuses},ensure_ascii=False,indent=2)); return out
