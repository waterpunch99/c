# Flink Web UI 블로그 캡처 안내

## 현재 실행해 둔 Job

- [실행 중인 Job 열기](http://localhost:8081/#/job/running/d0e248817edf46f641dce2a5b8789da4/overview)
- [Flink Web UI](http://localhost:8081)
- Job 이름: `parallelism-benchmark/ui_capture/ui_20261028T143228Z_5f7463a8`
- Job ID: `d0e248817edf46f641dce2a5b8789da4`
- Run ID: `ui_20261028T143228Z_5f7463a8`
- 실행 구분: **UI 캡처용 데모. 기존 실험 보고서의 성능 Run과 별도 실행.**

2026-10-28 23:36 KST에 REST로 Job·모든 Vertex RUNNING, Running Jobs=1, Producer 실행과 실제 Sink 진행을 확인했다. Kafka는 이번 요청에서 시작했고 기존 JobManager/TaskManager와 기존 JAR/Producer 이미지를 재사용했다. JAR·Java/Python 구현·Compose 설정은 수정하지 않았다.

Producer는 2026-10-28 23:34 KST부터 약 2시간, **2026-10-29 01:34 KST까지** 입력을 공급한다. Producer 종료 후에도 Unbounded Flink Job은 사용자가 Cancel할 때까지 RUNNING으로 남지만 입력이 없으면 지표는 Idle 상태가 된다. 캡처를 위해 Job과 서비스를 실행 상태로 남겨 두었다.

| 항목 | 이번 캡처용 실행 |
|---|---|
| Flink / Kafka | 2.2.1 / 4.2.1 |
| Kafka Partition | 8개, 신규 전용 Topic |
| Source P | 16 |
| Parse / Heavy / Aggregation / Sink P | 각각 4 |
| 입력 | 목표 100 records/s, Open-loop, Seed 42 |
| Key 분포 | Skewed, Hot Key 설정 비율 40% |
| Kafka Partition 선택 | userId와 독립적인 round-robin |
| Heavy / Keyed CPU 반복 횟수 | 0 / 200000 |
| Chaining / State | off / HashMap, maxParallelism 128 |
| Checkpoint | 5초 간격, timeout 60초 |
| TM 자원 | 1개, Docker quota 2 CPU, 2560 MiB, 16 Slots |

같은 Pipeline으로 Source P와 Keyed P를 별도로 설정하고, 실제 Source 할당과 Keyed 처리 분포를 한 화면 흐름에서 설명하기 위한 구성이다. 이전 실험의 동일 조건 재현이나 Full Benchmark로 표기하지 않는다. Execution Validity는 별도 성능 판정을 하지 않아 INCONCLUSIVE, Performance Stability는 UNKNOWN으로 기록했다.

## 어떤 화면을 캡처하면 좋은가?

### 1. Job Overview의 전체 Graph — 가장 먼저 첨부

Job을 열고 **Overview**에서 Source부터 Sink까지 전체 Graph와 Parallelism이 보이도록 캡처한다. 화면 폭이 부족하면 브라우저 배율을 약간 줄인다. 위쪽 Job 이름/상태와 각 Operator의 P를 함께 남긴다.

확인할 내용은 Source P=16, Parse/Heavy/Aggregation/Sink P=4, 모든 Operator RUNNING이다. Job 이름의 ui_capture를 남기면 원래 성능 Run과도 구분할 수 있다.

권장 캡션:

> 캡처용 실제 실행 Graph. Source Parallelism은 16, 나머지 Operator는 각각 4로 설정했다. Parallelism은 Operator별로 독립적으로 구성했다.

### 2. Kafka Source의 Subtasks와 할당 Count — Partition 제약 설명

Graph에서 **Source: Kafka Source**를 선택하고 **Subtasks**를 연다. 16개 Subtask가 보이도록 페이지 크기를 늘리고, 각 Subtask의 Records Sent/처리 건수를 함께 캡처한다. 입력이 계속 들어가는 상태에서 찍는다.

그 다음 해당 Source의 **Metrics**에서 `sourceAssignedPartitionCount`를 검색한다. 실제 등록된 전체 이름은 다음과 같다.

```text
Source__Kafka_Source.sourceAssignedPartitionCount
```

Subtask를 구분해 **0번의 Count=1과 6번의 Count=0**을 비교하면 할당과 단순 처리 건수의 차이를 설명하기 좋다. UI가 Subtask 지표를 합산해 표시하면 합계만으로 미할당을 판단하지 말고 Reader별 값이 보이도록 범위를 확인한다.

이번 Run에서 실제 관측한 할당은 다음과 같다. SourceReader의 실제 수락 콜백과 모든 Reader의 Count/Partition ID를 확인했다.

| Source Subtask | 실제 Partition |
|---|---|
| 0 / 1 / 2 / 3 / 4 / 5 | 각각 2 / 3 / 4 / 5 / 6 / 7 |
| **6~13** | **각각 Count=0, 미할당** |
| 14 / 15 | 각각 0 / 1 |

권장 캡션:

> Kafka Partition은 8개이고 Source Subtask는 16개다. 실제 할당 Count를 확인한 결과 8개 Reader만 Partition을 받았고 6~13번 Reader는 미할당이었다. 읽기 건수 0만으로 할당 여부를 추정하지 않았다.

이 인덱스는 이번 Topic/Run의 관측이다. 기존 보고서의 P=16 Run과 미할당 인덱스가 다른 것은 서로 다른 실행이기 때문이다.

### 3. Stateful Aggregation의 Subtasks — Hot Key 처리 편향 설명

Graph에서 **Stateful Aggregation**을 선택하고 **Subtasks**에서 4개 Subtask의 Records Received/Sent를 함께 캡처한다. 같은 순간의 누적 건수를 비교하며 개별 Counter가 계속 증가한다는 점을 고려한다.

이번 실행에서는 **Subtask 2**에서 Hot Key 처리를 관측했다. 필요하면 Metrics에서 다음 사용자 정의 Counter/Gauge를 검색한다.

```text
Stateful_Aggregation.aggregationMeasurementRecords
Stateful_Aggregation.aggregationMeasurementHotRecords
Stateful_Aggregation.hotKeyObservedSubtask
Stateful_Aggregation.hotKeyObservedKeyGroup
```

검증 시점의 실제 순차 REST 관측은 아래와 같다. 캡처 시점에는 숫자가 더 커지며, REST frame은 동시 Snapshot이 아니다.

| Subtask | 코호트 처리 Counter | Hot Key 처리 Counter |
|---:|---:|---:|
| 0 | 1821 | 0 |
| 1 | 1804 | 0 |
| **2** | **6579** | **4782** |
| 3 | 1813 | 0 |

`hotKeyObservedKeyGroup=86`, `hotKeyObservedSubtask=2`도 관측했다. 입력의 Hot 비율 40%는 설정값이며 시점별 실제 비율은 Counter로 확인한다. 이번 데모에서 Hot Key가 많은 Subtask를 확인한 것과 그 Subtask가 포화 병목이라는 주장은 구별한다.

권장 캡션:

> Aggregation P=4에서 Hot Key 처리는 Subtask 2 한 곳에 집중됐다. Kafka Partition 선택과 userId 분포를 분리했으며, keyBy 이후 처리 분포는 Subtask별 Counter로 확인했다. 이 화면은 처리 편향의 예시이며 최대 Capacity 측정은 아니다.

### 4. Checkpoints — 실제 Stateful Job 실행을 보완하는 그림

Job의 **Checkpoints** 탭에서 Completed 수와 최근 완료 Checkpoint의 Duration/State Size가 보이도록 캡처한다. 전체 Graph·Source·Aggregation 화면에 이어 선택적으로 첨부한다.

검증 시점에는 Completed=25, Failed=2였다. 초기 실패가 포함되어 있으므로 성공 건수만 잘라 “실패 0”으로 설명하지 않는다. 화면의 실제 숫자를 그대로 남긴다.

권장 캡션:

> 5초 간격 Checkpoint를 설정한 실제 실행 화면. 완료 이력과 초기 실패 이력을 함께 확인했다. 장애 복구나 외부 Sink Exactly-once를 검증한 화면은 아니다.

### 5. TaskManagers / Slots — 자원 설명이 필요할 때만 첨부

**Task Managers** 메뉴 또는 Dashboard에서 TM 1개, Total Slots=16을 캡처한다. 이 실행은 Slot Sharing으로 16 Slots를 사용하며 Docker TM quota는 2 CPU다. 화면의 Slot/논리 CPU 표기를 물리 CPU 할당으로 해석하지 않는다.

권장 캡션:

> TaskManager 1개에 16 Slots를 설정했지만 Docker CPU quota는 2 CPU로 고정했다. Slot 수와 물리 CPU 예산은 별도 조건이다.

**추천 조합은 1번 전체 Graph + 2번 Source 할당 + 3번 Aggregation 처리 분포의 세 장**이다. Checkpoints는 실제 상태 저장 실행을 보완할 때 추가한다. 낮은 부하의 Busy/Backpressure 화면은 포화나 병렬도 개선의 증거로 사용하지 않는다. Busy는 CPU Usage가 아니다. 지표 의미는 [Flink 공식 Metrics 문서](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/metrics/)를 따른다.

## 캡처용 실행 원본

[Run 디렉터리](../results/ui_demo/ui_20261028T143228Z_5f7463a8/manifest.json)에 목적·Job ID·설정·명령 인자·JAR SHA256·Producer 이름을 저장했다. [실제 검증 Snapshot](../results/ui_demo/ui_20261028T143228Z_5f7463a8/capture-verification.json), capture-subtasks.json, capture-environment.json, capture-job.json에 실제 할당·Hot Counter·CPU quota·이미지 ID를 보존했다.

기존 [실험 보고서](experiment-report.md)의 표·그림은 기존 Run의 원본을 따른다. 캡처 이미지에는 **“2026-10-28 캡처용 별도 실행”**이라고 표시하고, 이번 데모 Counter를 기존 보고서의 394건 등과 혼합하지 않는다. 데모에 Measurement 이름의 Counter가 있어도 원래 Controller의 Warm-up/Measurement/Drain으로 분리한 성능 Run을 뜻하지 않는다.

## 캡처가 끝난 뒤 중지

현재 실행만 중지하려면 저장소 루트에서 실행한다. 이 안내를 작성하면서 중지 명령을 실행하지 않았다.

```bash
docker stop --signal SIGINT --timeout 45 flink-ui-producer-ui_20261028T143228Z_5f7463a8
docker compose exec -T jobmanager /opt/flink/bin/flink cancel d0e248817edf46f641dce2a5b8789da4
```

다른 서비스와 Topic/결과 파일은 보존한다. Producer는 완료·중지 후 producer.json에 발행/ACK 결과를 기록하므로 실행 중에는 이 파일이 비어 있을 수 있다. 컨테이너는 자동 삭제하지 않아 로그를 확인할 수 있다.

## 같은 구성을 새 Run으로 실행하기

기존 Job을 위 명령으로 중지한 뒤 새 식별자로 실행한다. JAR과 Producer 이미지가 이미 준비된 현재 환경을 기준으로 한다. Kafka/Flink 서비스는 다음과 같이 시작한다.

```bash
docker compose up -d --wait --wait-timeout 180 kafka jobmanager taskmanager
ui_capture_run_id="ui_$(date -u +%Y%m%dT%H%M%SZ)"
ui_capture_topic="ui-demo-${ui_capture_run_id}"
mkdir -p "results/ui_demo/${ui_capture_run_id}"

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic "$ui_capture_topic" \
  --partitions 8 --replication-factor 1

python3 scripts/submit_job.py -- \
  --topic "$ui_capture_topic" --group-id "ui-group-${ui_capture_run_id}" \
  --experiment-id ui_capture --run-id "$ui_capture_run_id" \
  --source-parallelism 16 --parse-parallelism 4 --heavy-parallelism 4 \
  --aggregation-parallelism 4 --sink-parallelism 4 \
  --cpu-work-iterations 0 --keyed-cpu-work-iterations 200000 \
  --max-parallelism 128 --chaining false --state-backend hashmap \
  --checkpoint-interval-ms 5000 --checkpoint-timeout-ms 60000 \
  --latency-sample-every 10 --operator-cpu-sample-every 10

docker compose run -d --name "flink-ui-producer-${ui_capture_run_id}" producer \
  --topic "$ui_capture_topic" --experiment-id ui_capture --run-id "$ui_capture_run_id" \
  --target-rate 100 --duration 7200 --kafka-partitions 8 \
  --distribution skewed --hot-key-ratio 0.4 --seed 42 --payload-size 256 \
  --output "/results/ui_demo/${ui_capture_run_id}/producer.json"
```

새 Run에서는 submit_job.py가 출력한 실제 Job ID로 Web UI를 열고 중지해야 한다. 현재 Job ID를 재사용하지 않는다. 위 수동 재현은 manifest 자동 수집이나 Full 성능 판정을 수행하지 않는다.
