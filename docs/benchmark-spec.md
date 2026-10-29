# Flink Parallelism Benchmark 최종 명세

## 범위와 목적

이 문서는 전체 Phase 0~5의 계약이다. 현재 구현 요청은 **Phase 5**이며 Phase 0~4 구현·실측 기록을 재사용한다. 실험5·전체 CLI·최종 분석/재현 절차를 구현하고, 실제 검증 범위는 validation.md에 구분한다. Full Benchmark는 명시적 요청 없이 실행하지 않는다. 최종 질문은 “Parallelism을 어떤 근거로 결정하고 실제 데이터로 검증할 것인가?”이다. 과정은 측정 → 분석 → 예측 → 독립 검증 → 운영 기준 도출이다. 측정한 Subtask Capacity로 계산한 병렬도가 실제 적절한지를 블로그의 중심으로 삼는다.

최종 프로젝트는 Kafka/Flink 실제 스트림, 부하/Operator 병렬도 독립 제어, 유효성 검증, 5개 실험, Sustainable Capacity 추정, 병렬도 계산·실측 검증, CSV/JSON/PNG/한국어 Markdown, 재현 절차·한계를 제공해야 한다. 고정 자원에서의 병렬도 증가와 Hot Key 제약을 반드시 분석한다.

## 답해야 할 질문

1. P 증가 시 지속 처리량이 계속 증가하는가? 증가가 둔화되는 지점이 관측되는가?
2. 과도한 P의 Idle, 네트워크, State, Checkpoint, CPU 경합 비용은 무엇인가?
3. 실제 병목 Operator의 Subtask Sustainable Capacity를 추정할 수 있는가?
4. Peak Rate와 Target Utilization으로 필요한 P를 계산할 수 있는가?
5. 예측 P, 탐색 범위 내 최소 안정 P, 운영 추천 P는 얼마나 다른가?
6. 모든 Operator에 같은 P가 필요한가? 어떤 계측으로 병목을 찾는가?
7. Parallelism 부족과 Key Skew를 어떻게 구별하는가? Hot Key에 P 증가가 효과적인가?
8. Kafka Partition 수가 Source P에 어떤 제약을 주는가?
9. 실제 CPU/메모리 제한이 어떤 영향을 주는가?
10. Throughput 만족과 Latency/Checkpoint/Headroom을 포함한 운영 적합성은 어떻게 다른가?

## 신뢰성 계약

- 측정하지 않은 수치를 생성하지 않는다. Fixture/Mock과 실측 결과 경로를 분리한다.
- 결측을 0으로 바꾸지 않는다. null + Missing/Unsupported + 원인을 저장한다.
- 상관관계를 인과로 단정하지 않는다. 반증된 가설과 저성능 Run도 원본과 함께 보존한다.
- Producer가 목표 속도를 못 유지하면 Flink Capacity로 인정하지 않는다.
- Job 처리량/P를 모든 Operator의 Capacity로 해석하지 않는다.
- 실제 CPU quota/affinity/제한/배치를 확인한다. Busy Time은 CPU 사용량이 아니다.
- Restart/중복/누락 오염을 유일 Record Count로 해석하지 않는다. Exactly-once는 별도 검증 없이 주장하지 않는다.
- 테스트를 실행한 범위만 성공이라고 기록한다. 정상적으로 측정된 Backlog 증가를 오류로 바꾸지 않는다.
- 사전 등록한 임계값을 결과에 맞춰 사후 수정하지 않는다. 변경 실험은 새 조건/Run으로 남긴다.

### 독립적인 두 상태

| 축 | 값 | 의미 |
|---|---|---|
| Execution Validity | VALID | 설정·계측·생산·격리 전제 충족 |
| | INVALID | Producer 미달, 설정 불일치, 데이터 오염 등 비교 불가 |
| | ERROR | Job/수집/실행 실패 |
| | INCONCLUSIVE | 근거 부족 또는 노이즈로 판정 불가 |
| Performance Stability | STABLE | 설정 부하를 지속 처리 |
| | UNSTABLE | 유효하게 관측된 Backlog/지연 지속 악화 등 |
| | UNKNOWN | 안정성 판정 불가 |

VALID + UNSTABLE Run은 탐색의 상한 근거로 보존한다. 짧은 Smoke의 계통 검증 성공은 VALID + UNKNOWN으로 기록하며 Capacity 측정으로 간주하지 않는다.

## 버전과 인프라

Java 17, Flink 2.2.1, Kafka Connector 5.0.0-2.2, Kafka KRaft, Maven, Docker Compose, Python 3를 기본으로 사용한다. Phase 1 선택 버전과 근거는 architecture.md에 기록한다. 모든 직접 의존성과 이미지 버전을 고정하며 전이 의존성은 Maven 그래프로 기록한다. Prometheus, Pandas, Matplotlib은 해당 Phase에서 구체 버전을 확인·고정한다. Grafana는 선택이다.

Compose는 Kafka/JM/TM, Topic 자동 생성, 준비 확인, Web UI, 설정 기반 제출, 실제 Job ID 확인, Producer/Job 안전 종료를 지원해야 한다. 프로세스/TCP 생존과 메타데이터/REST/Slot/Source 실제 준비를 구분한다. Run별 Checkpoint 경로 및 신규 State 정책을 명시한다.

환경 메타데이터: OS/Architecture, Java/Maven/Python/Flink/Kafka, CPU 총수/affinity/cgroup quota, Docker CPU·메모리 한도, JM/TM/Heap/Managed·Network 메모리 설정, TM 수·Slot 수·실사용 Slot·실제 Subtask 배치, Backend, Checkpoint, Git SHA, Chaining, Clock 차이 및 측정 오차. 실제 SHA가 없으면 null이다.

기본 `fixed-resource`: 모든 P 조건의 CPU/메모리 예산 동일. 선택 `resource-scaled`와 결과 혼합 금지. Operator P 합계는 CPU나 Slot 사용량이 아니다. Slot Sharing을 함께 기록한다. CPU Oversubscription은 물리 리소스 포화와 논리 P 제약을 구별하여 기록한다. Producer/Kafka/Flink가 Host CPU를 공유하면 경합 가능성을 기록한다.

## Pipeline 계약

Kafka Source → Parse → Heavy CPU → keyBy(userId) → Stateful Aggregation → Measurement Sink. 각 Operator에 고유 Name/UID, 별도 P, Subtask 메트릭. 고정 maxParallelism, Chaining on/off, Backend, Checkpoint Interval/Timeout, Heavy/Keyed CPU 작업 강도, Partition 수, 분포를 제어한다. 핵심 계통은 입력 1건→출력 1건이며 Window 축약은 제외한다.

이벤트 필수: eventId/userId/producerTimestamp/payload/experimentId/runId. generationPhase/sequence 등으로 측정 구간·중복을 식별한다. Payload Size와 Schema는 비교 조건에서 동일하다. 파싱 실패는 Counter와 오류로 드러나며 정상 건수에 조용히 포함하지 않는다.

Heavy 연산은 Sleep 없이 입력 기반의 결정적 CPU 계산. 강도를 설정하고 결과를 Sink까지 사용하여 제거를 방지하며 루프 중 불필요한 할당을 피한다. 계산 정확성 및 Record별 시간 분포를 테스트/계측한다. 고정 연산량도 JIT/GC/CPU에 따라 시간이 달라진다.

Aggregation은 Keyed State의 User별 Count를 갱신하고 매 입력을 출력한다. Keyed CPU 강도·Backend·maxP·Checkpoint를 제어한다. Skew 조건에서 실제 병목이 되었는지는 메트릭으로 검증한다.

Sink는 외부 DB 없이 완료 Counter, 생성 구간별 Count, 결정적 Latency 샘플, 결과 Checksum을 제공한다. Sink numRecordsOut을 완료 건수라고 가정하지 않는다. 기본 Sink는 Production 외부 I/O 성능을 대표하지 않는다.

## Producer 계약

Python Open-loop를 우선한다. 최소 옵션: target-rate/duration/total-records/distribution/number-of-users/payload-size/seed/kafka-partitions/experiment-id/run-id/hot-key-ratio/producer-workers/partition-strategy. 기본 User 범위 1..1,000,000, payload 256, seed 42. Skew는 User=1의 40%, 나머지는 다른 Key에 분산한다. 실제 생성 비율을 측정한다.

Monotonic Clock의 예정 시각을 기준으로 목표 발행을 시도한다. Target/Scheduled/Actual Sent/Kafka ACK Rate, Error/Retry, Send Latency, Queue·Buffer 압력을 구분한다. Flink 진행에 맞춰 의도적으로 속도를 낮추지 않는다. BufferFull 등 실제 생산 실패를 숨기지 않는다. Rate 미달 사유는 유효성에 반영한다.

User 분포와 Kafka Partition 분포를 별도로 측정한다. Kafka 메시지 Key를 자동으로 userId로 설정하지 않는다. 명시적 Partition 선택 등 독립 전략을 사용하며 Partition별 발행/ACK 수를 기록한다. Uniform/Skewed에서 Partition 불균형이 설정 허용 범위를 넘으면 INVALID 또는 INCONCLUSIVE. 생성 로직만으로 실제 Subtask 균등 부하를 가정하지 않는다.

Phase 2 Producer/Kafka Preflight는 목표 Rate 유지 가능성을 먼저 확인한다. 불충족이면 Capacity 측정을 시작하지 않거나 INVALID. Java Producer 전환은 정확성/성능 측정 이점이 있을 때 근거를 기록한다.

## 계측과 구간

기본 간격 5초. 지원 여부와 의미를 실환경으로 확인한다.

Job: Target/Actual/ACK Rate, Source Read, Sink Completed, Sustainable Throughput, E2E p50/p95/p99, Kafka Backlog/Slope, Checkpoint Duration/Failure, Restart, TM Process CPU/Memory/Throttling/GC, 실제 Slot, Run 상태.

Subtask: numRecordsIn/Out, In/OutPerSecond, busy/idle/backPressuredTimeMsPerSecond, Operator P, Index, TM ID, 누적 Record Count와 구간별 Rate. 가능 시 Thread CPU Time 근사치/GC/State 크기. Thread CPU를 Operator에 귀속할 때 Chaining/스레드/배치 영향과 오차를 기록한다. TM Process CPU를 Subtask CPU라고 표시하지 않는다. Busy와 CPU 동일시 금지.

진단용 Chaining off를 지원하고 모든 비교에 같은 정책을 적용한다. Chaining on 확인 실험은 별도 조건이다.

E2E = Sink wall clock − Producer 생성 wall clock. 내부 처리 지연과 구분하고 Clock 차이와 오차를 확인한다. 결정적 샘플 또는 병합 Histogram으로 p50/p95/p99/count/min/max/시간 추세. Subtask p99 평균을 Job p99로 쓰지 않는다. 샘플링 off/on 비교로 오버헤드를 평가할 수 있어야 한다.

Kafka 지표는 Partition Log End Offset, Source Current Read Offset, Group Committed Offset, KafkaSource pendingRecords를 구분한다. Commit Lag를 실제 Read Lag로 취급하지 않는다. Partition 단위 원시 값, 처리량, 시간 추세를 함께 저장한다. 실제 Partition Assignment는 지원 메트릭/로그로 관측하며 예상 배치를 결과로 출력하지 않는다.

Full 기본 Warm-up 60초 / Measurement 180초 / 조건당 최소 3회. 순서: 서비스 준비 → 신규 Run → Job 제출 → Source 진행 확인 → Warm-up → Backlog 확인 → Measurement → Producer 중지 → Drain → 최종 미처리/Offset 확인 → Job 종료·저장. Smoke는 짧은 계통 검증이다.

별도 Count: Measurement 생성 수, Measurement wall-clock 안의 완료 수, Measurement 생성분 중 Drain 완료 수, Drain Timeout, 최종 미처리 수. Drain 처리량을 Measurement Throughput에 합산하지 않는다. 생성 코호트 Latency에 미처리 이벤트가 남으면 incomplete로 표시한다.

Run마다 Topic/Group/Job/State/experimentId/runId/결과 경로를 격리한다. State restore·Offset 재사용을 기본으로 하지 않는다. Restart 시 중복 오염을 평가한다. 전체 ID 보관이 불가능한 규모는 정확성 검증 비용·범위를 명시한다.

## Sustainable Throughput 판정 및 탐색

최대 순간 처리율과 지속 처리율을 구분한다. 목표 생산 유지, Sink 처리율, Backlog 장기 증가, Latency 악화, Checkpoint 성공, 리소스 포화, Restart/장애를 함께 판단한다. 임계값은 버전 관리 Config에 둔다. Producer 초기 ±2%는 이 실험의 초기 기준이며 Flink 표준이 아니다. Sink/Produced 비율 하나로 STABLE이라고 판정하지 않는다.

Backlog 회귀: records/sec, 구간, 기울기, 가능 시 표준오차/CI, 초기·최종·누적 변화, 적용 임계값을 저장한다. 노이즈로 불명확하면 INCONCLUSIVE + UNKNOWN. 결과에 적용 기준, 관측 값, 이유, 오염 요인, 한계가 있어야 한다.

Rate는 낮은 값부터 단계 증가, 포화 구간 확인, 필요 시 범위 축소. 최대 시도 수·Rate 허용 오차를 설정한다. 모든 시도 보존. Stable 하한/Unstable 상한/해상도/미탐색 범위를 기록하며 검색 상한에서 Stable한 값을 절대 최대라고 주장하지 않는다.

## 실험 계약

### 1. Parallelism 증가

Partition 32, 동일 Operator P 후보 1/2/4/8/16, fixed-resource. 1-A는 P별 Capacity Discovery; 5000→10000→15000→20000은 조정 가능한 예시. 1-B는 동일 입력률 비교. Actual Production/Sink/Capacity/Backlog/Busy/Idle/Backpressure/Latency/CPU/Memory/Checkpoint/Validity를 측정한다. 그래프: P vs Sustainable Throughput, p95/p99, CPU, Busy/Idle/Backpressure, Lag Slope. 개선율·Knee 후보를 탐색하되 존재를 가정하지 않는다. Producer/물리 CPU 포화를 P 자체의 한계와 구분한다.

### 2. Capacity로 계산하고 독립 검증

Job Capacity, 실제 병목 Operator Capacity, Per-subtask Estimate를 구분한다. 병목, Source/Sink 선포화, Subtask 균등성, CPU/Memory, P=2/4/8별 추정 편차와 fixed-resource 경합을 검증한다. 충분한 증거가 없으면 Operator Capacity 추정을 거부하거나 조건부 표시한다.

`P_i = ceil(λ_i / (C_i × U_i))`. Peak Rate, 해당 Operator의 지속 가능한 Subtask Capacity, Target Utilization으로 계산. 출력 P, 부하/Subtask, Headroom, 가정. λ_i는 해당 Operator 입력률이며 출력 축약 시 Job 입력률과 다를 수 있다. U는 CPU/Busy 비율이 아니다. 40000/(7000×0.7)→9는 설명용 예시로만 표시한다.

독립 검증 Run에서 Peak Rate 고정, P*−2/P*−1/P*/P*+1/P*+3 등 양수 후보. 학습 Run과 검증 Run 분리; 분포·Workload 유지, Seed/순서 변경 정책 기록. Throughput, Lag/Latency/Backpressure/Busy/Idle/CPU/Checkpoint/안정성 비교.

예측 P, 실제 탐색한 후보 중 최소 Stable P, 운영 추천 P 구분. 전역 최소라고 주장하지 않는다. 운영 P는 SLO/Checkpoint/Headroom/자원까지 고려. Capacity Table, Operator별 추정·포화, 계산 과정, 예측 vs 관측, P별 추세, Calculation Error, U=0.6/0.7/0.8 민감도, 적용·실패 조건 제공.

### 3. Operator별 병목

Heavy 병목을 의도하되 검증한다. A=(2,2,2,2,2), B=(8,8,8,8,8), C=(2,2,8,4,2). C에서 다른 Operator 선포화를 사전 확인하고 조정·이유를 기록한다. Operator별 Rate/Busy/Idle/Backpressure/Subtask 부하, E2E/Latency, CPU core-seconds, Memory, 실제 Slot 측정. 최초 병목, 집중 Scale-out 효과, 불필요 Idle, 동일 Throughput 자원 효율 분석. Upstream Backpressure가 Downstream 영향일 가능성을 구별한다.

### 4. Data Skew

Uniform vs 40% HOT_USER, P=2/4/8/16. Keyed CPU를 높여 Aggregation 병목을 관측하고 Heavy 선포화를 방지한다. Kafka Partition 부하는 두 분포에서 비교 가능해야 한다. Hot Key 담당 Subtask를 실측으로 식별한다. 하나의 Key가 keyBy에서 한 Subtask에 속하는 제약을 검증한다.

Subtask Count/Rate/Busy/Idle/Backpressure/Thread CPU 근사, E2E/p95/p99/Lag, 실제 Hot Ratio, Partition Count. Max/Mean, CV, Hot Traffic, Load 분포. 평균 0 처리; Backpressure 후 관측 처리율을 실제 유입 분포와 동일시하지 않는다. 생성 분포와 완료 분포를 함께 비교한다.

그래프: Uniform/Skewed Throughput, p99, P별 Subtask Rate/Busy Heatmap, Imbalance vs Throughput, Hot Subtask Load vs P. 원인이 Source Partition인지 함께 확인. 선택 Salting/Two-stage는 원래 집계 의미 정확성 테스트; 매 입력 최신 누적값/순서 의미가 달라질 수 있음을 공개한다.

### 5. Partition과 Source

Partition 8, Source P=2/4/8/16, Downstream 충분 용량. Source 및 Subtask Rate/Busy/Idle, 실제 Partition Assignment, pendingRecords/Lag/E2E. 다중 Partition 할당, P>Partition 미할당 Subtask, Throughput 변화, Downstream과 다른 P를 관측한다. 예상 배치를 관측으로 표시하지 않는다. Watermark 사용 시 Idle Source의 영향 검토; 기본 계통은 noWatermarks이다.

## 공정성·자동화·저장

Payload/CPU/Keyed Work/Kafka/Checkpoint/Backend/maxP/Chaining/리소스/Warm-up/Measurement/분포 정책 고정. Full 최소 3회, 랜덤 순서 옵션, 모든 개별 Run 보존. Throttle/GC/OOM/Checkpoint Fail/Restart/Producer Error/Host 부하 변화 기록. State Cardinality/크기 가능 시 기록; JIT·초기 State 생성 영향을 설명한다.

CLI: `python scripts/run_experiment.py --experiment 1|2|3|4|5|all --mode smoke|full`. 실험별 옵션 repetitions/target-rate/peak-input-rate/target-utilization/distribution/hot-key-ratio/seed/output-dir/config, fixed-resource만 지원한다. all은 별도 Suite 설정의 실험별 overrides를 사용하며 모호한 전역 옵션은 거부한다. 환경→Topic→Job ID→Producer→구간→수집→판정→저장→정리→다음 Case 자동화. 명시적 요청 없이 전체 Full 실행 금지.

`results/experiment_N/run_ID/{metadata.json,samples.csv,subtasks.csv,summary.json}`, 실험별 summary.csv. 각 Run 경로와 파일은 신규 생성만 허용. Raw/Summary 분리, 실패 Run 보존. 메타데이터에는 위 환경 항목과 모든 Workload, 분포 실측, Producer/Source/Sink Rate, Job ID, 구간 길이, 상태·판정 이유, Missing, Resource Warnings 포함.

분석 CLI `scripts/analyze_results.py --experiment N`: Summary CSV, 비교표, PNG, 한국어 리포트. 반복 Mean/Std/Min/Max/유효 반복 수/개별 Run. 적은 반복의 통계 불확실성 공개. Latency는 p50/p95/p99/추세/샘플 수/코호트 완료율; 작은 표본·검열된 미처리 이벤트의 한계. 그림 생성 불가 시 상태/오류를 남기며 임의 데이터 금지.

## 블로그 계약

각 실험 한국어 리포트는 질문/가설/환경/지표/실측 결과·그림/원인 분석/실무 기준/한계 8개 절을 가진다. 미실행 결과는 `Not Executed`. 인과 미검증 원인은 가능성으로 표시한다. 중심은 Capacity 기반 계산과 실측 검증, fixed-resource와 Hot Key 한계이다.

## 테스트·최종 완료 체크리스트

Unit/Integration/Full을 구분한다. 최소 검증: Uniform, Skewed, Partition 분포; Producer Rate/ACK; Heavy 계산; Keyed State 정확성; 병렬도 공식; Metric Parser/Missing; Sustainable/Run Status; Summary 통계; 덮어쓰기 방지; 실제 Kafka/Flink E2E; 종료·정리. 가능 시 eventId 중복·누락 검증 범위를 정의한다.

- [x] Compose/Kafka/Flink 실제 실행·Job 제출·Producer ACK
- [x] Target Rate 설정/검증·Operator P·CPU 강도·Uniform/Skewed·Partition 분포
- [x] Flink/Kafka/Subtask 계측·Warm-up/Measurement/Drain·Run 격리
- [x] Sustainable 판정 로직·실험 1~5 자동화·Capacity 계산/독립 검증 코드 및 Unit
- [ ] Full 실측 Sustainable Capacity 추정과 계산된 P의 독립 장시간 검증·최소 안정/운영 P 도출
- [x] Raw/CSV/Summary·한국어 README·한계/재현법
- [x] 실험1~5 실제 PNG·한국어 분석/통합 블로그 리포트 (원본 선택·실패·미관측 근거는 validation.md)
- [x] 실제 E2E Smoke·미검증 표시

실행 자동화가 구현된 것과 모든 실험이 성능 검증을 통과한 것은 구분한다. Phase5 전체 축소 Suite는14 Run을 실제 실행했으나11 VALID/2 INVALID/1 ERROR였으며 실패를 보존했다. 별도 실험5 Rate20의 네 Run은 VALID+UNKNOWN이다. Full은 미실행이며 Smoke를 Capacity로 승격하지 않는다. 최종 분석 출력과 Phase별 완료 근거는 validation.md를 따른다.
