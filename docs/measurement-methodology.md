# Phase 2 측정 방법

이 문서는 단일 계측 Run의 정의다. `run_once.py`는 실험 1~5 탐색기가 아니며 Smoke 결과로 최대 Capacity를 계산하지 않는다. 기본값/임계값은 `configs/experiments/phase2.json`, 실제 적용값은 각 Run metadata와 summary에 보존한다.

## 실행과 격리

서비스 준비 → 다른 활성 Job 없음/Slot 확인 → 환경 수집 → 별도 Producer/Kafka Preflight → 신규 Topic/Group/State/Job → Partition당 실제 준비 이벤트 → Warm-up → Measurement → Producer 중지/ACK flush → Drain → 정확성/판정 → Job Cancel·Topic 삭제 확인 순서다. 서비스와 Checkpoint 파일은 남긴다. 각 실행은 exclusive UUID 디렉터리를 사용해 성공/실패 결과를 보존한다. SIGINT/SIGTERM·수집 실패에도 해당 Run의 자원 정리를 시도하고 오류를 기록한다.

## Producer와 구간

고정된 Monotonic 예정 시점으로 부하를 발생시킨다. Scheduled는 예정 슬롯, Generated는 직렬화 대상 이벤트, Sent는 Producer에 수락된 이벤트, ACK는 성공 콜백이다. Queue full/expired/error/retry·호출시간·ACK시간·buffer pressure를 별도로 저장한다. 처리 완료를 기다려 다음 부하를 줄이지 않는다. 구간이 끝난 예정 슬롯을 뒤늦게 보내지 않는다. Kafka Partition은 userId와 독립적으로 선택한다.

이벤트에 generationPhase와 고정 Measurement 시작/끝 wall timestamp를 포함한다. `[start,end)` 범위다. Sink는 다음 계수를 구분한다.

| 계수 | 의미 |
|---|---|
| completedRecords | 전체 완료, 준비 이벤트 포함 |
| measurementCompletedRecords | Measurement에서 생성된 코호트의 최종 완료 |
| measurementWindowCompletedRecords | Measurement 시간에 완료된 모든 코호트 |
| measurementWindowCohortCompletedRecords | Measurement 생성 코호트 중 같은 시간 내 완료 |

현재 summary의 measurement_sink_throughput은 마지막 계수 / 설정 Measurement 초다. 전체 시간 내 완료 수도 따로 제공한다. Drain 완료는 measurementCompletedRecords - measurementWindowCohortCompletedRecords이며 Throughput에 합산하지 않는다. ACK도 생성 코호트 ACK율과 실제 Measurement 시간 내 ACK율을 구분한다. 최종 미완료 = Sent - 코호트 완료이며 Latency 통계의 검열 여부를 표시한다. 카운터는 비트랜잭션 관측값이다. Restart가 있으면 INVALID, 큰 실행에서 유일 건수라고 주장하지 않는다. Smoke의 전체 ID 검증은 최대 10,000개 Bitset으로 제한한다.

## Offset와 메트릭

Admin API LEO는 다음 append offset이다. 사용자 Source deserialize Gauge는 실제 ConsumerRecord offset+1이다. backlog는 두 값의 차이이며 전체 Partition 값이 있어야 합계를 생성한다. committed offset lag는 별도이며 체크포인트 진행과 연결된 값으로 실제 read 진행을 대신하지 않는다. pendingRecords도 독립 메트릭이다.

REST가 비동기로 캐시한 값을 제공하므로 fetch interval을 1초로 설정하고 모든 Source/Sink Subtask에 timestamp Gauge를 둔다. 조회 후 새 Admin LEO/Commit를 수집한다. 시각 차이 허용 한도는 3초이며 불일치·미등록·충돌·음수 차이는 null이다. API 호출 전체 시간, Source/REST/Admin 시각 차이를 Raw에 남긴다. 이 방식은 분산 원자적 snapshot이 아니다. Deserialize Gauge는 consumer prefetch offset보다 Pipeline 입력 진행에 가깝지만 sink 완료 위치는 아니다.

Operator Name/UID와 실제 Job ID/Graph P/Subtask/TM/Slot 배치를 조회한다. 표준 task metric과 operator metric이 함께 있으면 task scope를 우선하며 Chaining off를 전제로 한다. Busy/Idle/Backpressure와 TM Process CPU를 별도로 저장한다. TM/Kafka/Producer cgroup usage delta를 실제 quota로 정규화하고 Throttle/Memory/OOM을 기록한다. 논리 Slot 수를 CPU 분모로 사용하지 않는다. Unsupported Subtask CPU와 State-size는 이유를 기록하고 Checkpoint state_size는 원시값으로 남긴다.

## Latency와 Checkpoint

Latency는 Sink wall 시각 - Producer 생성 wall 시각이다. 코호트의 결정적 Sequence 샘플을 log2 Histogram에 넣는다. 전체 Subtask Bucket 합에서 p50/p95/p99 상한을 구하고 Bucket 차분으로 구간 추세를 계산한다. 개별 p99를 평균하지 않는다. Sample count/min/max·negative sample을 보존한다. Host에서 컨테이너 요청 전후 시각으로 Clock offset 범위를 만들고 JM/TM/Producer의 모든 범위가 허용 한도 내인지 확인한다. 이는 동기화/정확도를 증명하지 않는다.

Checkpoint 완료/실패는 최초/최종 Measurement sample 사이 증가량이며 시작 전 실패·Drain 완료는 누적 총계로 별도 보존한다. 관측 경계 공백이 있으므로 측정한 샘플 범위를 함께 기록한다. Startup 실패를 Measurement 실패로 오해하지 않는다.

## 독립 판정

1. 실행/API 수집/정리 오류는 ERROR + UNKNOWN.
2. Preflight/실제 Producer Rate·ACK·Partition 전제 불충족, 설정 불일치, 오염/Restart는 INVALID + UNKNOWN.
3. 필수 근거 부족은 INCONCLUSIVE + UNKNOWN.
4. 정상 관측된 backlog 회귀 하한 > slope 기준 및 누적 증가 > delta 기준이면 VALID + UNSTABLE. Latency 악화나 측정 구간 Checkpoint 실패도 불안정 근거다.
5. Smoke/짧은 실행은 VALID + UNKNOWN. 최대 Capacity 증거로 인정하지 않는다.
6. 충분한 Measurement 기간/샘플/커버리지, backlog 상한/변화량, Sink 비율, 신뢰 가능한 Latency, Checkpoint, 리소스 근거, Drain 완료가 모두 충족돼야 Full에서 STABLE.

회귀 slope의 단위는 records/sec, Latency slope는 ms/sec다. 초기/최종 값, 구간, SE, 정규 근사 95% 구간을 저장한다. 자기상관과 작은 표본 때문에 엄밀한 신뢰 구간으로 해석하지 않는다. STABLE이더라도 그 Run의 부하는 지속 처리 가능 하한이며 최대 Capacity가 아니다. CPU 포화/Throttle 경고는 Capacity 원인 분석 및 운영 Headroom 판단에 남긴다. 설정 파일은 실행 전에 읽으며 결과를 유리하게 만들기 위해 사후 변경하지 않는다.

## 지원 근거와 범위

Phase 4에서는 Aggregation의 Measurement/Hot 코호트와 wall window Counter, 실제 Hot 처리 후 Subtask/Key Group Gauge, 선택적 Heavy/Keyed 동기 CPU 샘플을 추가했다. 원래의 Run 상태/구간/Offset 판정은 유지한다. 비교 전제는 별도 기록하고 단일 Key 배치와 성능 병목을 구분한다. 정의·신선도·생성/발행/ACK 대조·샘플 CPU의 귀속 한계는 [phase4-design](phase4-design.md)을 따른다.

Phase5는 실제 SourceReader 할당 콜백 Count/Partition ID와 Reader timestamp를 추가했다. 모든 Reader의 Count(실제0 포함)·신선도·전체 Partition의 유일한 소유자·읽기 Gauge를 대조한다. Offset 읽기 진행과 할당 여부를 구분하며 누락 Count를0으로 채우지 않는다. Assignment epoch 변경/Restart/동적 Partition은 별도 불확실성이다. pendingRecords는 지연 등록이어서 미할당 Reader에서 결측일 수 있다. 전체 합계는 관측 Reader 일부의 합계로 대체하지 않는다. CSV와 Raw의 per-reader 값을 함께 보존한다. 기준·Adapter 범위는 [phase5-design](phase5-design.md)을 따른다.

[Flink 2.2 metrics](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/metrics/)의 task/operator 메트릭, [KafkaSource 문서](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/)의 committed offset/Checkpoint 구분, [Confluent Python Admin API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)의 list_offsets/list_consumer_group_offsets를 참고했다. 최신 Python API 문서의 버전은 설치 버전과 다를 수 있으므로 실제 pinned 2.12.2 이미지에서 호출/ACK를 검증했다. Source read Gauge의 의미는 직접 구현한 deserialize 지점으로 정의한다. 실제 관측/차단/한계는 validation.md를 따른다. Prometheus 구성·다중 worker·Chaining 비교·큰 ID audit·장시간/반복 성능 실험은 현재 검증 범위 밖이다.
