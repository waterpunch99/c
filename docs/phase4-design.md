# Operator 병목과 Data Skew 실험

현재 범위는 Phase 4의 실험3/4다. Phase 2의 실제 Run 실행/메트릭/구간/정리, Phase 3의 통계·결측·물리 예산 비교 원칙을 재사용한다. 실험5/all CLI는 Phase 5까지 미지원이다. `configs/experiments/phase4.json`에서 실행 전에 설정과 임계값을 등록하고 Session에 frozen 설정을 저장한다.

## 실험 3

| Case | Source | Parse | Heavy | Aggregation | Sink |
|---|---:|---:|---:|---:|---:|
| Low Baseline | 2 | 2 | 2 | 2 | 2 |
| Uniform High | 8 | 8 | 8 | 8 | 8 |
| Bottleneck-focused | 2 | 2 | 8 | 4 | 2 |

같은 Payload/분포/Rate/연산량/메모리·CPU 예산을 제공한다. Heavy200000/Keyed1000 iterations는 의도한 작업량이지 병목이 검증된 값이 아니다. 모든 Operator P 합을 Slot/CPU 수로 사용하지 않는다. 실제 occupied Slot과 Task 배치를 따로 저장한다.

비교 전에 Focused Graph와 같은 P, Heavy/Keyed iterations=0인 독립 Run으로 Source/Parse/Sink의 저작업량 부하 전달을 점검한다. Sink 비율·Backlog CI 상한·Source/Parse/Sink 최대 Busy를 확인한다. 충분한 Full 안정성 근거일 때만 `QUALIFIED_LOW_WORK_ONLY`, Smoke에서 조건을 충족해도 `PROVISIONAL_SMOKE_ONLY`다. 실패/불확실 결과도 보존하고 실제 Case는 탐색적 측정으로 계속 기록한다. 사전 점검의 다른 Workload를 비교 통계에 섞지 않는다.

실제 비교에서도 병목 후보와 다른 Operator Busy/Backpressure를 반드시 확인한다. 최초 병목을 주장하려면 Baseline에서 Heavy 후보가 관측돼야 하며, Focused Case에 Source 등 다른 후보가 나타나면 이를 숨기지 않는다. 후보가 없거나 사전 점검이 불확실하면 의도한 병목 기반 개선 주장을 보류한다. Workload 변경이 필요하면 설정 파일에 이유를 기록하고 새로운 Session에서 A/B/C 전체를 다시 실행한다. 결과에 맞춰 같은 Session의 작업량을 바꾸지 않는다.

Operator별 처리율·Busy/Idle/Backpressure 및 Subtask 지표, Sink/Latency/Lag, TM core-seconds/메모리/실제 Slot을 비교한다. Core-seconds는 실제 cgroup 수집 시각이 Measurement 안인 snapshot 사이 usage 차분으로, 전체 Measurement 기간으로 외삽하지 않는다. 관측 범위와 평균 사용 CPU 코어도 제공한다. snapshot 시간 범위/실제 처리 건수도 함께 봐야 하며 서로 다른 수집 커버리지의 core-seconds만으로 자원 효율을 단정하지 않는다. TM Process 비용에는 GC/runtime/다른 Operator가 포함된다.

## 실험 4

Full P=2/4/8/16, Smoke P=2/4. 동일 P에서 Uniform/Skewed를 같은 Seed와 고정 예산으로 비교한다. 반복 r은 Seed+r을 사용하고 선택적으로 실행 순서를 섞는다. 같은 Seed라도 분포 알고리즘의 RNG 호출 방식이 달라 동일 이벤트 목록을 의미하지 않는다. 비교 목적은 분포 정책/Workload/Partition 부하를 통제하는 것이다.

Heavy0/Keyed200000 iterations로 Stateful Aggregation 작업을 상대적으로 높인다. 실제 병목 여부는 최대 Subtask Busy·Downstream Backpressure·다른 Operator와의 차이로 판정한다. Uniform과 달리 Skew에서 평균 Busy가 낮아도 Hot Subtask가 포화될 수 있으므로 최대치를 확인한다. 기본 기준 Busy750ms/sec, Backpressure150ms/sec 이하, 타 Operator 최대 Busy보다100ms/sec 이상 높은 단일 후보를 관측한다. 이는 초기 실험 기준이며 CPU 원인 증명이 아니다.

Kafka Partition은 userId와 독립적인 명시적 round-robin/random 전략이다. Measurement 생성 Hot count, Sent Hot count, ACK Hot count 및 실제 Partition ACK count를 저장한다. 실제 Hot 비율은 생성 hot/generated, 기본 Skew 기대값0.4, 허용 절대 차이0.05이며 최소100개 생성 근거가 필요하다. Uniform의 기대 Hot 비율은1/number-of-users다. 이 허용폭을 확률적 신뢰 구간이나 일반 표준이라고 주장하지 않는다.

각 Run의 Partition Max/Mean 기준을 확인한 뒤, 쌍의 정규화 ACK 비율의 Total Variation `0.5 × Σ|p_i-q_i|`를 비교한다. 기본 한도0.02. Actual Graph P/Seed와, 통제한 분포 외 Workload/Artifact/물리 예산 fingerprint가 같아야 한다. 부적합은 INVALID, 근거 부족은 INCONCLUSIVE로 비교를 보류한다. 단일 `--distribution skewed` 실행은 해당 관측만 저장하며 Uniform 비교는 Not Executed다. 비교 유효성은 Run의 두 실행/안정성 상태와 구분해 추가 저장한다.

## 관측한 Hot 배치와 부하

Aggregation Counter는 Measurement 생성 코호트만 세며 Warm-up/준비 이벤트를 제외한다. 최종 코호트 수, Hot 코호트 수, `[measurementStart,measurementEnd)` 시간 내 처리 수/Hot 처리 수를 따로 제공한다. 실제 userId=1을 처리한 뒤에만 `hotKeyObservedSubtask`/`hotKeyObservedKeyGroup` Gauge가 값을 제공하고 그 전에는-1이다. 예상 hash 배치를 관측 결과로 출력하지 않는다.

최종 Subtask cohort sum=Producer Sent, Hot sum=Producer ACK Hot, Restart0, 신선한 Gauge를 확인해야 `OBSERVED_SINGLE_SUBTASK`다. 동일 Key가 여러 Subtask에 관측되면 오염 상태로, 부족/미완료/오래된 snapshot은 INCONCLUSIVE다. Uniform에서 실제 Hot 이벤트가 없으면 NO_HOT_EVENTS이며 배치를 지어내지 않는다.

처리 코호트 Count와 snapshot 사이 output rate 각각에 Max/Mean/CV를 계산한다. 평균0이면 비율을 null로 둔다. Backpressure가 이미 있으면 처리율의 분포는 실제 유입 Key 분포를 대표하지 않으므로 생성 Hot 비율과 최종 처리 완료율을 함께 제공한다. Hot Subtask 그래프는 실제 확인된 Subtask의 측정 시간 내 코호트/Hot 처리량을 사용하고 Drain을 제외한다.

## CPU 샘플의 의미

`--operator-cpu-sample-every 10`은 Heavy/Keyed Operator 각 Subtask의 로컬 레코드 순번마다10번째 동기 본문을 ThreadMXBean으로 측정한다. Heavy는 payload seed+CPU 계산, Aggregation은 State 접근·업데이트+Keyed CPU 계산이며 out.collect와 계측 Counter는 제외한다. Kafka/Parse/Sink에는 이 CPU 샘플을 귀속하지 않는다. 평균 비용은 sampledOperatorCpuNanos delta / operatorCpuSampleCount delta이며 유효 샘플이 없거나 미지원/꺼짐/Counter Reset이면 null과 이유다. 전체 Subtask CPU %, Process CPU, Flink Busy와 다른 근사치다.

새 플래그 기본0은 CPU 샘플을 끈다. 기존 latency-sample-every는 Latency/Heavy 연산 wall-time Histogram의 Sequence 샘플이며 별개다. CPU 샘플의 대표성·ThreadMXBean 비용·on/off 정량 비교는 미검증이다. 정확한 Subtask 전체 CPU 귀속을 구현했다고 표시하지 않는다. 공식 정의는 [Java17 ThreadMXBean](https://docs.oracle.com/en/java/javase/17/docs/api/java.management/java/lang/management/ThreadMXBean.html)을 따른다. keyBy 구조는 [Flink keyed state 설명](https://flink.apache.org/2017/07/04/a-deep-dive-into-rescalable-state-in-apache-flink/)과 실제 Counter를 구분해 해석한다.

## 실행과 분석

```bash
docker compose run --rm builder -B -ntp verify
docker compose build producer
make unit-analysis
python3 scripts/run_experiment.py --experiment 3 --mode smoke
python3 scripts/run_experiment.py --experiment 4 --mode smoke
docker compose run --rm analyzer --experiment 3
docker compose run --rm analyzer --experiment 4
```

실험3 Smoke4 Run(사전 점검1+A/B/C), 실험4 Smoke4 Run(2P×2분포), 조건당1회. Full은 각각12 Run(3회 사전점검+3Case×3회),24 Run(4P×2분포×3회), Load/Drain 상한3720초/7440초이며 Setup/API/정리 비용이 추가된다. Full은 이 작업에서 실행하지 않고 다음 명령의 계획만 확인한다.

```bash
python3 scripts/run_experiment.py --experiment 3 --mode full --dry-run
python3 scripts/run_experiment.py --experiment 4 --mode full --dry-run
```

명시적으로 Full을 실행할 때는 dry-run을 검토한 뒤 같은 명령에서 `--dry-run`을 제거한다. workload 변경은 `--config` 부분 JSON의 experiment_3/experiment_4 CPU iterations를 사용한다. `--target-rate`, `--parallelisms`(실험4), `--distribution`(실험4), `--hot-key-ratio`, `--seed`, `--repetitions`, `--random-order`를 지원한다.

`results/experiment_N/session_UUID/`는 frozen session.json, append-only runs.jsonl, diagnostics.json, 비교 전제/후보/Run 상태의 session_summary.json, 개별 Raw Run을 저장한다. `analysis_UUID/`는 summary/repeat_statistics/operator_diagnostics/subtask_diagnostics/latency_trend CSV, comparisons/diagnostics/environments JSON, 실측 PNG와 한국어8절 report.md를 보존한다. CPU 비용은 불변 Raw에서 cgroup 시각 경계를 적용해 재계산하며 분석 코드 SHA256/버전/보정 적용 사실을 analysis.json에 기록한다. 전제 실패/결측 차트는 analysis.json에 Not Executed다. 모든 Heatmap의 결측/미설정 Subtask는 마스킹하고 실제0만0으로 표시한다.
