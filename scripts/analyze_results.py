"""Phase 3 measured session tables/PNG/Korean report; never impute missing performance."""
import argparse
import csv
import json
import math
import statistics
import uuid
from pathlib import Path
from lab import ROOT, save_json
from result_store import read_jsonl
from capacity_analysis import measured_samples


def write_csv(path, rows, fields):
    with Path(path).open('x', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='raise'); writer.writeheader(); writer.writerows(rows)


def flatten(row, diagnostic):
    o = row.get('observations', {}); latency = o.get('latency', {})
    operators = diagnostic.get('operators', {})
    def mean_metric(name):
        vals = [v.get(name) for v in operators.values()]
        return statistics.mean(vals) if len(vals) == 5 and all(v is not None for v in vals) else None
    cp = (o.get('checkpoint_latest') or {}).get('completed') or {}
    tm = diagnostic.get('resources', {}).get('taskmanager', {})
    return {'run_id': row['run_id'], 'role': row['role'], 'parallelism': row['parallelism'], 'target_rate': row['target_rate'],
            'condition_fingerprint': diagnostic.get('fingerprint'),
            'mode': row['mode'], 'execution_validity': row['execution_validity'], 'performance_stability': row['performance_stability'],
            'actual_produced_rate': o.get('producer_validation', {}).get('actual_sent_rate'),
            'sink_throughput': o.get('measurement_sink_throughput'), 'sustainable_throughput': row.get('sustainable_throughput'),
            'latency_p50_ms': latency.get('p50'), 'latency_p95_ms': latency.get('p95'), 'latency_p99_ms': latency.get('p99'),
            'latency_sample_count': latency.get('sample_count'), 'cohort_completion_fraction': o.get('cohort_completion_fraction'),
            'latency_cohort_complete': o.get('latency_cohort_complete'),
            'backlog_slope': o.get('backlog_regression', {}).get('slope'), 'final_read_backlog': o.get('final_read_backlog'),
            'busy_ms_per_second': mean_metric('busy_ms_per_second'), 'idle_ms_per_second': mean_metric('idle_ms_per_second'),
            'backpressure_ms_per_second': mean_metric('backpressure_ms_per_second'),
            'tm_cpu_quota_utilization': tm.get('cpu_quota_utilization', {}).get('mean'),
            'tm_throttled_period_ratio': tm.get('throttled_period_ratio', {}).get('max'),
            'checkpoint_latest_duration_ms': cp.get('end_to_end_duration'), 'checkpoint_measurement_failures': o.get('checkpoint_failures'),
            'restart_count': o.get('restart_count'), 'bottleneck_candidate': diagnostic.get('bottleneck_candidate'),
            'resource_saturation': '; '.join(diagnostic.get('resource_saturation', [])),
            'reasons': '; '.join(row.get('reasons', [])), 'run_directory': row['run_directory']}


METRICS = ['actual_produced_rate', 'sink_throughput', 'sustainable_throughput', 'latency_p50_ms', 'latency_p95_ms', 'latency_p99_ms',
           'latency_sample_count', 'cohort_completion_fraction', 'backlog_slope', 'busy_ms_per_second', 'idle_ms_per_second',
           'backpressure_ms_per_second', 'tm_cpu_quota_utilization', 'tm_throttled_period_ratio', 'checkpoint_latest_duration_ms']


def repeat_statistics(records):
    """Only valid observed runs; one repetition has unknown SD, not SD=0."""
    import pandas as pd
    frame = pd.DataFrame(records)
    output = []
    if frame.empty: return output
    for keys, group in frame.groupby(['role', 'parallelism', 'target_rate', 'mode', 'condition_fingerprint'], dropna=False, sort=True):
        valid = group[group.execution_validity == 'VALID']
        for metric in METRICS:
            vals = pd.to_numeric(valid[metric], errors='coerce').dropna()
            output.append(dict(zip(['role', 'parallelism', 'target_rate', 'mode', 'condition_fingerprint'], [None if pd.isna(k) else k for k in keys])) |
                          {'metric': metric, 'total_runs': len(group), 'valid_runs': len(valid), 'observed_runs': len(vals),
                           'stable_runs': len(valid[valid.performance_stability == 'STABLE']),
                           'mean': float(vals.mean()) if len(vals) else None,
                           'std': float(vals.std(ddof=1)) if len(vals) > 1 else None,
                           'min': float(vals.min()) if len(vals) else None, 'max': float(vals.max()) if len(vals) else None})
    return output


def gain_analysis(searches, threshold, bracket_resolution_limit=.2):
    points = sorted((int(p), s) for p, s in searches.items() if s['stable_input_lower_bound'] is not None)
    gains = []; knees = []
    for (p0, s0), (p1, s1) in zip(points, points[1:]):
        gain = s1['stable_input_lower_bound'] / s0['stable_input_lower_bound'] - 1
        bounded = all(s['status'] == 'BRACKETED' and s['resolution_records_per_second'] / s['stable_input_lower_bound'] <= bracket_resolution_limit for s in [s0, s1])
        gains.append({'previous_parallelism': p0, 'parallelism': p1, 'stable_lower_bound_relative_gain': gain,
                      'both_bracketed': bounded, 'interpretation': 'gain in discovered lower bounds; not proof of causal scaling'})
        if bounded and 0 <= gain < threshold: knees.append(p1)
    return {'gains': gains, 'knee_candidates': knees, 'status': 'CANDIDATES_ONLY' if knees else 'NOT_IDENTIFIED',
            'reason': 'No knee assumed; requires sufficiently resolved capacity brackets, independent resource interpretation'}


def charts(out, aggregates, summary, experiment):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    statuses = {}
    role = 'fixed_input' if experiment == 1 else ('validation' if (summary.get('prediction') or {}).get('status') == 'CONDITIONAL_PREDICTION' else 'uncalibrated_peak_probe')
    fingerprints = {r['condition_fingerprint'] for r in aggregates if r['role'] == role}
    def plot(name, series, ylabel, title):
        available = {label: sorted(points) for label, points in series.items() if points}
        if not available:
            statuses[name] = {'status': 'Not Executed', 'reason': 'No eligible measured values'}; return
        fig, ax = plt.subplots(figsize=(7, 4))
        for label, points in available.items():
            ax.plot([p for p, v, sd in points], [v for p, v, sd in points], marker='o', label=label)
            for p, v, sd in points:
                if sd is not None: ax.errorbar(p, v, yerr=sd, color='gray', capsize=3)
        ax.set(xlabel='Parallelism', ylabel=ylabel, title=title + ' [' + summary['mode'] + ']')
        ax.legend(); ax.grid(alpha=.2); fig.tight_layout(); fig.savefig(out / name, dpi=150); plt.close(fig)
        statuses[name] = {'status': 'Generated', 'series': list(available), 'note': 'Points are observed run statistics; bars show SD when >=2 values'}
    def metric_series(metric):
        if len(fingerprints) != 1: return []  # Different physical budgets are not a common P curve.
        return [(r['parallelism'], r['mean'], r['std']) for r in aggregates if r['role'] == role and r['metric'] == metric and r['mean'] is not None]
    plot('sustainable_throughput.png', {'observed stable processing lower bound': [(int(p), s['observed_sustainable_throughput_lower_bound'], None) for p, s in summary['searches'].items() if s.get('observed_sustainable_throughput_lower_bound') is not None and summary.get('search_conditions_comparable', True)]}, 'records/sec', 'Observed sustainable processing lower bound (not maximum)')
    plot('throughput.png', {'sink within measurement': metric_series('sink_throughput'), 'actual produced': metric_series('actual_produced_rate')}, 'records/sec', 'Fixed input comparison')
    plot('latency.png', {'run p95 mean': metric_series('latency_p95_ms'), 'run p99 mean': metric_series('latency_p99_ms')}, 'ms, log2 upper bounds', 'Run quantiles (not merged repetition quantiles)')
    plot('cpu.png', {'TM quota utilization': metric_series('tm_cpu_quota_utilization')}, 'fraction of actual CPU quota', 'TaskManager cgroup CPU')
    plot('busy_idle_backpressure.png', {m: metric_series(m + '_ms_per_second') for m in ['busy', 'idle', 'backpressure']}, 'ms/sec, mean of observed subtasks/operators', 'Task timing (not CPU)')
    plot('backlog_slope.png', {'read backlog regression': metric_series('backlog_slope')}, 'records/sec', 'Read backlog trend')
    if experiment == 2:
        v = summary.get('validation') or {}; pairs = [(label, v.get(k)) for label, k in [('predicted', 'predicted_required_parallelism'), ('measured candidate minimum', 'measured_minimum_stable_parallelism'), ('operational', 'recommended_operational_parallelism')]]
        if any(n is not None for _, n in pairs):
            fig, ax = plt.subplots(figsize=(6, 4)); known = [(l, n) for l, n in pairs if n is not None]
            ax.bar([l for l, n in known], [n for l, n in known]); ax.set(ylabel='Parallelism', title='Conditional prediction and tested minimum'); fig.tight_layout(); fig.savefig(out / 'prediction_vs_observed.png', dpi=150); plt.close(fig)
            statuses['prediction_vs_observed.png'] = {'status': 'Generated', 'missing': [l for l, n in pairs if n is None]}
        else: statuses['prediction_vs_observed.png'] = {'status': 'Not Executed', 'reason': 'No measured calibration or stable independent validation'}
    return statuses


def report(out, config, summary, records, statuses, gains, environment):
    experiment = summary['experiment']; frozen = config['frozen_config']
    question = '병렬도가 지속 처리량과 지연·리소스에 어떤 영향을 주는가?' if experiment == 1 else '측정한 Subtask Capacity로 계산한 병렬도가 실제로 적절했는가?'
    def val(v): return 'Missing' if v is None else f'{v:.4g}' if isinstance(v, float) else str(v)
    lines = [f'# 실험 {experiment} 분석 ({summary["mode"]})', '', '## 1. 실험 질문', '', question, '',
             '## 2. 가설', '', '병렬도 증가가 처리 여력을 높일 수 있지만 고정 CPU·Kafka·Key 분포·Checkpoint 경합에 따라 효과가 달라질 수 있다. 가설이 관측과 일치한다고 가정하지 않는다.', '',
             '## 3. 실험 환경', '', f'Session: `{summary["session_id"]}`. 자원 모드: fixed-resource. 원본 환경·이미지·Quota·Graph·Clock은 각 Run metadata.json을 따른다.', '',
             '```json', json.dumps(frozen, ensure_ascii=False, indent=2), '```', '',
             '대표 실제 Run 환경 (각 Run의 차이와 상세 Clock은 metadata.json/environments.json 참조):', '',
             '```json', json.dumps(environment[0] if environment else None, ensure_ascii=False, indent=2), '```', '',
             '## 4. 측정 지표', '', 'Producer 목표/Actual/ACK 정합성, 측정 시간 내 Sink 완료, read backlog 회귀, 코호트 Latency Histogram, Busy/Idle/Backpressure, 실제 cgroup CPU/Throttle 및 Checkpoint를 함께 사용한다. Busy는 CPU가 아니다.', '',
             '## 5. 실험 결과', '', f'자동화 상태: {summary["status"]}. Run {len(records)}개. 다음 표는 개별 Run의 관측값이며 실패 Run도 보존한다.', '',
             '| Run | 역할 | P | 목표 Rate | 유효성 | 안정성 | Sink Rate | p99(ms) | Lag slope | Latency samples |',
             '|---|---|---:|---:|---|---|---:|---:|---:|---:|']
    for r in records:
        lines.append('| ' + ' | '.join(val(r[k]) for k in ['run_id', 'role', 'parallelism', 'target_rate', 'execution_validity', 'performance_stability', 'sink_throughput', 'latency_p99_ms', 'backlog_slope', 'latency_sample_count']) + ' |')
    if summary['mode'] == 'smoke': lines += ['', '**Sustainable Capacity / 계산한 병렬도의 실측 검증: Not Executed.** Smoke의 UNKNOWN을 STABLE로 바꾸지 않았다. 관측 처리량/P를 Operator Capacity로 사용하지 않았다.']
    lines += ['', '반복 통계는 VALID Run의 Mean/표본 Std/Min/Max/유효·실제 관측 횟수다. 1회 반복의 Std는 Missing이다. Run별 p99의 평균은 전체 이벤트 p99가 아니다. 코호트 완료율·개별 Histogram을 함께 확인한다.', '', '탐색 결과:', '', '```json', json.dumps(summary['searches'], ensure_ascii=False, indent=2), '```']
    if experiment == 2:
        lines += ['', 'Capacity 근거·계산·민감도:', '', '```json', json.dumps(summary.get('prediction'), ensure_ascii=False, indent=2), '```', '', '독립 검증:', '', '```json', json.dumps(summary.get('validation'), ensure_ascii=False, indent=2), '```']
    for name, status in statuses.items():
        lines += ['', f'![{name}]({name})' if status['status'] == 'Generated' else f'{name}: **Not Executed** — {status.get("reason", "Missing")}']
    lines += ['', '## 6. 원인 분석', '', 'Knee 분석: ' + json.dumps(gains, ensure_ascii=False), '',
              'Operator 포화 후보와 자원 경고는 operator_diagnostics.csv를 따른다. 높은 Upstream Backpressure만으로 해당 Operator가 원인이라고 단정하지 않는다. CPU quota 포화·Throttle과 처리량의 상관관계는 원인 증명이 아니다. 계측 자체의 비용도 포함된다.', '',
              '## 7. 실무 적용 기준', '', 'Operator의 실제 입력률과 근거가 있는 조건부 Capacity로 P를 계산하고, 별도 Run의 Peak 부하에서 Lag/Latency/Checkpoint/Headroom을 검증한다. 예측 P·탐색한 후보 중 최소 안정 P·운영 추천 P를 구분한다. 물리 CPU가 늘지 않는 Scale-out과 Hot Key 제약을 함께 검토한다. Hot Key 비교는 후속 실험이며 이번 결과로 검증하지 않았다.', '',
              '## 8. 실험의 한계', '', '짧은 Smoke·적은 반복의 통계 불확실성, REST/Admin 비원자적 snapshot, log2 Histogram 상한 근사, Clock 오차, JIT/State 생성, 외부 I/O 없는 Sink, 비트랜잭션 Counter를 고려한다. 미완료 코호트의 p99는 검열돼 불완전하다. 최대 Capacity나 전역 최소 P를 주장하지 않는다. Full 장시간 분기는 실제 실행 기록이 있을 때만 검증 완료로 표시한다.']
    with (out / 'report.md').open('x', encoding='utf-8') as f: f.write('\n'.join(lines) + '\n')


def analyze(session, experiment):
    if experiment==5:
        from phase5_analysis import analyze as analyze_phase5
        return analyze_phase5(session,experiment)
    if experiment in [3,4]:
        from phase4_analysis import analyze as analyze_phase4
        return analyze_phase4(session,experiment)
    import pandas as pd
    import matplotlib
    import numpy as np
    session = Path(session).resolve(); config = json.loads((session / 'session.json').read_text())
    summary = json.loads((session / 'session_summary.json').read_text())
    if config['experiment'] != experiment or summary['experiment'] != experiment: raise ValueError('Wrong experiment')
    rows = read_jsonl(session / 'runs.jsonl'); diagnostics = json.loads((session / 'diagnostics.json').read_text())
    if len({r['run_id'] for r in rows}) != len(rows): raise ValueError('Duplicate Run ID in manifest')
    if any(r['mode'] != summary['mode'] for r in rows): raise ValueError('Do not mix Smoke/Full')
    search_fingerprints = {diagnostics[rid]['fingerprint'] for s in summary['searches'].values() for a in s['attempts']
                           for rid in a['run_ids'] if rid in diagnostics}
    summary['search_conditions_comparable'] = len(search_fingerprints) == 1
    records = [flatten(r, diagnostics.get(r['run_id'], {})) for r in rows]
    out = session / ('analysis_' + uuid.uuid4().hex); out.mkdir(exist_ok=False)
    fields = list(records[0]) if records else ['run_id', 'role', 'parallelism']
    write_csv(out / 'summary.csv', records, fields)
    aggregates = repeat_statistics(records)
    write_csv(out / 'repeat_statistics.csv', aggregates, ['role', 'parallelism', 'target_rate', 'mode', 'condition_fingerprint', 'metric', 'total_runs', 'valid_runs', 'observed_runs', 'stable_runs', 'mean', 'std', 'min', 'max'])
    op_rows = []
    for rid, d in diagnostics.items():
        for op, v in d['operators'].items():
            op_rows.append({'run_id': rid, 'operator': op, **v, 'bottleneck_candidate': d['bottleneck_candidate'] == op,
                            'resource_saturation': '; '.join(d['resource_saturation'])})
    write_csv(out / 'operator_diagnostics.csv', op_rows, list(op_rows[0]) if op_rows else ['run_id', 'operator'])
    capacities = (summary.get('prediction') or {}).get('capacity', {}).get('entries', [])
    write_csv(out / 'capacity_estimation.csv', capacities, list(capacities[0]) if capacities else ['parallelism', 'operator', 'per_subtask_estimate', 'status'])
    trends = []; environments = []
    for r in rows:
        directory = session / r['run_directory']; metadata = json.loads((directory / 'metadata.json').read_text())
        env = metadata.get('environment', {})
        environments.append({'run_id': r['run_id'], 'host': env.get('host'),
                             'flink': env.get('flink_overview'), 'kafka_version': env.get('kafka_version'),
                             'operator_parallelism': metadata['config']['operator_parallelism'],
                             'containers': [{'service': c.get('Config', {}).get('Labels', {}).get('com.docker.compose.service'),
                                             'image': c.get('Image'), 'cpu_quota_nanocpus': c.get('HostConfig', {}).get('NanoCpus'),
                                             'memory_limit_bytes': c.get('HostConfig', {}).get('Memory')} for c in env.get('docker_inspect', [])]})
        for s in measured_samples(directory, metadata):
            interval = s.get('latency_interval', {})
            trends.append({'run_id': r['run_id'], 'parallelism': r['parallelism'], 'seconds_since_measurement': (s['wall_time_ms'] - metadata['plan']['measurement_start_wall_ms']) / 1000,
                           'interval_p95_ms': interval.get('p95'), 'interval_p99_ms': interval.get('p99'), 'interval_samples': interval.get('sample_count'),
                           'cohort_prefix_p99_ms': s.get('latency', {}).get('p99')})
    write_csv(out / 'latency_trend.csv', trends, ['run_id', 'parallelism', 'seconds_since_measurement', 'interval_p95_ms', 'interval_p99_ms', 'interval_samples', 'cohort_prefix_p99_ms'])
    statuses = charts(out, aggregates, summary, experiment)
    if trends and any(r['interval_p99_ms'] is not None for r in trends):
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(7, 4))
        for rid in dict.fromkeys(r['run_id'] for r in trends):
            series = [r for r in trends if r['run_id'] == rid and r['interval_p99_ms'] is not None]
            if series: ax.plot([r['seconds_since_measurement'] for r in series], [r['interval_p99_ms'] for r in series], marker='.', label=rid[-8:])
        ax.set(xlabel='Seconds since measurement', ylabel='Interval p99 ms (log2 upper bound)', title='Sampled cohort interval latency [' + summary['mode'] + ']'); ax.legend(); fig.tight_layout(); fig.savefig(out / 'latency_trend.png', dpi=150); plt.close(fig)
        statuses['latency_trend.png'] = {'status': 'Generated'}
    else: statuses['latency_trend.png'] = {'status': 'Not Executed', 'reason': 'No observed interval latency samples'}
    save_json(out / 'environments.json', environments)
    t = config['frozen_config']['evidence_thresholds']
    gains = gain_analysis(summary['searches'], t['knee_relative_gain'], t['maximum_capacity_bracket_relative_width'])
    if not summary['search_conditions_comparable']:
        gains.update(knee_candidates=[], status='INCOMPARABLE', reason='Different/missing workload/resource fingerprints; no shared scaling claim')
    report(out, config, summary, records, statuses, gains, environments)
    save_json(out / 'analysis.json', {'source_session': str(session), 'versions': {'pandas': pd.__version__, 'matplotlib': matplotlib.__version__, 'numpy': np.__version__},
                                    'charts': statuses, 'gain_analysis': gains, 'search_conditions_comparable': summary['search_conditions_comparable'],
                                    'valid_runs': sum(r['execution_validity'] == 'VALID' for r in rows)})
    print(json.dumps({'analysis_directory': str(out), 'charts': statuses}, ensure_ascii=False, indent=2))
    return out


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--experiment', choices=['1', '2', '3', '4', '5', 'all'], required=True); p.add_argument('--session', type=Path)
    p.add_argument('--mode', choices=['smoke','full'],default='smoke',help='Filter latest sessions; never mix modes')
    p.add_argument('--suite',type=Path,help='all: select exact sessions from suite manifest')
    p.add_argument('--results-dir',type=Path,default=ROOT/'results')
    args = p.parse_args()
    if args.experiment=='all':
        if args.session: p.error('all accepts --suite, not a single --session')
        from blog_report import analyze_all
        output=analyze_all(args.results_dir,args.mode,args.suite)
        manifest=json.loads((output/'report.json').read_text())
        raise SystemExit(1 if any(v['status']=='ERROR' for v in manifest['experiments'].values()) else 0)
    if args.suite: p.error('--suite applies to all only')
    args.experiment=int(args.experiment)
    session = args.session
    if session is None:
        candidates = [f for f in (args.results_dir / ('experiment_' + str(args.experiment))).glob('session_*/session.json')
                      if json.loads(f.read_text())['mode']==args.mode and (f.parent/'session_summary.json').exists()]
        if not candidates: p.error('Not Executed: no measured session; no charts fabricated')
        session = max(candidates, key=lambda x: json.loads(x.read_text())['started_wall_ms']).parent
    analyze(session, args.experiment)
