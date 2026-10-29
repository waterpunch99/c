"""Consolidate explicitly selected measured sessions; no benchmark or capacity inference."""
import csv
import hashlib
import json
import os
import uuid
from pathlib import Path
from lab import ROOT, save_json
from result_store import read_jsonl
from analyze_results import write_csv


def select_sessions(results, mode, suite=None):
    results=Path(results).resolve()
    if mode not in ['smoke','full']: raise ValueError('Unknown mode')
    selected={}
    if suite:
        suite=Path(suite).resolve()
        manifest=json.loads((suite/'suite_summary.json').read_text())
        if manifest['mode']!=mode: raise ValueError('Do not mix Smoke/Full')
        for record in manifest['sessions']:
            experiment=record['experiment']
            if experiment not in range(1,6) or experiment in selected or record['mode']!=mode: raise ValueError('Invalid suite manifest')
            sid=record['session_id']
            if not sid or Path(sid).name!=sid or sid in ['.','..']: raise ValueError('Invalid session ID')
            logical=Path('experiment_'+str(experiment))/sid
            relative=Path(record.get('session_relative_directory',str(logical)))
            if relative!=logical: raise ValueError('Invalid experiment/session relative path')
            # Suite roots travel between host /home/... and analyzer /workspace, or archive copies.
            local=suite.parent.parent/relative
            selected[experiment]=local.resolve()
    else:
        for experiment in range(1,6):
            candidates=[]
            for file in (results/('experiment_'+str(experiment))).glob('session_*/session.json'):
                data=json.loads(file.read_text())
                if data['mode']==mode and (file.parent/'session_summary.json').exists(): candidates.append((data['started_wall_ms'],file.parent))
            if candidates: selected[experiment]=max(candidates,key=lambda x:x[0])[1]
    return selected


def build_report(results, mode, selected, analyses, errors=None, suite=None):
    out=Path(results)/'reports'/('report_'+uuid.uuid4().hex); out.mkdir(parents=True,exist_ok=False)
    manifest={}; records=[]; summaries={}; seen=set(); errors=errors or {}
    for experiment in range(1,6):
        session=selected.get(experiment); analysis=analyses.get(experiment)
        if session is None:
            manifest[str(experiment)]={'status':'Not Executed','reason':'No selected measured '+mode+' session'}; continue
        session=Path(session); config=json.loads((session/'session.json').read_text()); summary=json.loads((session/'session_summary.json').read_text())
        if config['experiment']!=experiment or summary['experiment']!=experiment or config['mode']!=mode or summary['mode']!=mode: raise ValueError('Wrong experiment or mixed modes')
        summaries[experiment]=summary
        for r in read_jsonl(session/'runs.jsonl'):
            if r['run_id'] in seen or r['mode']!=mode: raise ValueError('Duplicate run ID/mixed modes')
            seen.add(r['run_id']); o=r.get('observations',{}); latency=o.get('latency',{})
            records.append({'experiment':experiment,'session_id':summary['session_id'],'run_id':r['run_id'],
                'role':r['role'],'parallelism':r['parallelism'],'target_rate':r['target_rate'],'mode':mode,
                'execution_validity':r['execution_validity'],'performance_stability':r['performance_stability'],
                'sink_measurement_throughput':o.get('measurement_sink_throughput'),'sustainable_throughput':r.get('sustainable_throughput'),
                'latency_p50_ms':latency.get('p50'),'latency_p95_ms':latency.get('p95'),'latency_p99_ms':latency.get('p99'),
                'latency_sample_count':latency.get('sample_count'),'cohort_completion_fraction':o.get('cohort_completion_fraction'),
                'backlog_slope':o.get('backlog_regression',{}).get('slope'),'restart_count':o.get('restart_count'),
                'cleanup_error_count':len(r.get('cleanup_errors',[])),
                'raw_directory':str((session/r['run_directory']).resolve())})
        manifest[str(experiment)]={'status':'Analyzed' if analysis else 'ERROR','execution_status':summary['status'],
            'session_directory':str(session.resolve()),'session_id':summary['session_id'],
            'analysis_directory':str(Path(analysis).resolve()) if analysis else None,'analysis_error':errors.get(experiment),
            'session_sha256':hashlib.sha256((session/'session.json').read_bytes()).hexdigest(),
            'summary_sha256':hashlib.sha256((session/'session_summary.json').read_bytes()).hexdigest()}
    write_csv(out/'summary.csv',records,list(records[0]) if records else ['experiment','run_id'])
    prediction=summaries.get(2,{}).get('prediction'); validation=summaries.get(2,{}).get('validation')
    capacity_status='Not Executed' if mode=='smoke' or not prediction or prediction.get('status')!='CONDITIONAL_PREDICTION' else 'Conditional prediction; independent validation evidence below'
    save_json(out/'report.json',{'mode':mode,'suite':str(Path(suite).resolve()) if suite else None,'selection_policy':'explicit suite manifest' if suite else 'latest finished session per experiment in requested mode; no cross-experiment performance pooling',
        'experiments':manifest,'run_count':len(records),'capacity_validation_status':capacity_status,'prediction':prediction,'validation':validation,
        'limitations':['No common throughput curve or capacity inferred across experiments/workloads/artifacts',
            'Missing experiments have no charts; failed/unstable runs retained; Full never mixed with Smoke']})
    lines=['# Flink Parallelism은 어떤 기준으로 결정해야 하는가?','','## 1. 실험 질문','',
        '측정한 Subtask Capacity로 계산한 Parallelism이 실제로 적절했는가? 물리 자원이 고정되거나 Hot Key가 있을 때 판단은 어떻게 달라지는가?','',
        '## 2. 가설','','지속 가능한 Operator Capacity와 Peak Input Rate로 1차 P를 계산할 수 있을 것이다. 고정 CPU의 경합, 단일 Hot Key, Kafka Partition 제약은 이 계산의 적용 조건을 제한할 것이다. 가설의 검증 여부는 아래 실측 근거를 따른다.','',
        '## 3. 실험 환경','',f'선택 Mode: **{mode}**. 선택한 Session만 사용한다. 각 실험의 Workload·Graph·환경·코드 Hash가 다를 수 있어 성능 수치를 합쳐 하나의 Capacity 곡선을 만들지 않는다. 명시적 선택/Hash는 report.json, 실제 환경은 각 분석의 environments.json과 Run metadata.json을 따른다.','',
        '## 4. 측정 지표','','Target/Actual/ACK, 측정 wall-clock Sink 완료(Drain 제외), 코호트 Latency Histogram·Sample Count·완료율, Backlog 회귀/CI, Checkpoint, Busy/Idle/Backpressure, 실제 CPU quota/Throttle/메모리·Slot·Subtask 배치, Key/Partition의 실제 분포를 함께 평가한다. Busy는 CPU가 아니며 U는 Capacity 대비 목표 사용 비율이다.','',
        '## 5. 실험 결과','','| 실험 | 실제 Session | 실행 상태 | Run 수 | 분석 리포트 |','|---|---|---|---:|---|']
    for experiment in range(1,6):
        item=manifest[str(experiment)]; summary=summaries.get(experiment)
        link='Not Executed'
        if item.get('analysis_directory'):
            relative=os.path.relpath(Path(item['analysis_directory'])/'report.md',out); link='[실측 분석]('+relative+')'
        elif item['status']=='ERROR': link='ERROR: '+str(item.get('analysis_error'))
        lines.append('| '+str(experiment)+' | '+item.get('session_id','Not Executed')+' | '+item.get('execution_status','Not Executed')+' | '+str(summary['run_count'] if summary else '—')+' | '+link+' |')
    lines+=['',f'선택된 실제 Run: {len(records)}개. 개별 관측과 상태·원본 경로는 summary.csv, 조건별 반복 Mean/Std/Min/Max·PNG는 각 실험 리포트에서 확인한다. 실패·INVALID·UNSTABLE도 보존하며 비교에 쓸 수 있는 조건은 각 리포트의 전제 판정을 따른다.','',
        '**측정한 Capacity로 계산한 P의 적절성: '+capacity_status+'**','',
        '```json',json.dumps({'prediction':prediction,'independent_validation':validation},ensure_ascii=False,indent=2),'```','',
        '예측 P, 탐색한 후보 중 최소 안정 P, 운영 추천 P는 서로 다르다. 미관측 값은 null이다. Smoke의 설정 Peak Probe를 계산된 P의 검증이라고 부르지 않는다.','',
        '## 6. 원인 분석','',
        '고정 물리 자원: P가 커져도 Docker CPU quota가 늘지 않는다. 실제 cgroup 사용·Throttle·GC·Slot/배치와 Operator/Subtask 메트릭을 함께 본다. 처리율 정체와 P의 상관관계만으로 P 자체의 한계를 확정하지 않는다.','',
        'Hot Key: 생성 Key 분포와 Kafka Partition 분포를 독립 검증하고, 실제 Aggregation Hot Counter와 Subtask 배치를 확인한다. 한 Key의 처리 위치를 관측한 것과 그 Key가 포화 병목이라는 주장은 구별한다. 실험4의 실제 병목 후보·비교 전제와 Hot 배치는 해당 리포트를 따른다.','',
        'Kafka Partition: 읽기 흔적만으로 미할당 Reader를 단정하지 않는다. 실험5의 실제 할당 콜백, Count=0과 모든 Partition 소유자 관측을 함께 사용한다. Source P와 Downstream P를 별도 결정한다.','',
        '## 7. 실무 적용 기준','',
        '1. 대표 Workload·State·Checkpoint·물리 자원에서 Producer 능력과 관측 유효성을 검증한다.',
        '2. Lag/Latency/Checkpoint를 만족하는 Sustainable 부하 구간을 탐색하고 실제 병목 Operator를 식별한다.',
        '3. 복수 P의 균등 Subtask 관측과 리소스 경합 검증을 통해 조건부 Capacity C_i를 추정한다.',
        '4. `P_i = ceil(λ_i / (C_i × U_i))`로 후보를 계산하고 별도 Run에서 같은 Peak Rate를 적용한다.',
        '5. 탐색 범위의 최소 안정 P와 Latency SLO·Checkpoint·Headroom을 만족하는 운영 P를 구분한다. 단일 Hot Key 및 Partition 제약에 맞는 설계를 확인한다.','',
        '## 8. 실험의 한계','',
        'Smoke는 실행·계측·정리·분석 검증이며 Capacity/장시간 안정성/증설 효과의 증거가 아니다. Full 실행 기록이 없으면 Sustainable Capacity·계산 오차·최소/운영 P·U 민감도 실측은 Not Executed다. 짧은 반복의 통계 불확실성, JIT·State 성장·공유 Host, REST/Admin 비원자성, Clock·Histogram 근사, CPU 샘플 대표성·계측 오버헤드, 비트랜잭션 완료 Counter, 외부 I/O 없는 Sink를 고려한다. Production에 수치를 그대로 일반화하지 않는다. Event-time/Watermark·복구·장기 State 증가·외부 Sink는 별도 검증이 필요하다.']
    (out/'blog-report.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    return out


def analyze_all(results=ROOT/'results',mode='smoke',suite=None):
    from analyze_results import analyze
    selected=select_sessions(results,mode,suite); analyses={}; errors={}
    for experiment,session in sorted(selected.items()):
        try: analyses[experiment]=analyze(session,experiment)
        except Exception as exc: errors[experiment]=repr(exc)
    out=build_report(results,mode,selected,analyses,errors,suite)
    print(json.dumps({'report_directory':str(out),'missing_experiments':[e for e in range(1,6) if e not in selected],'analysis_errors':errors},ensure_ascii=False,indent=2))
    return out
