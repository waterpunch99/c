# Flink Parallelism Lab

**“Flink Streaming Job의 Parallelism은 어떤 기준으로 결정해야 하는가?”**를 실제 부하와 계측으로 검증하는 프로젝트입니다. 전체 목표는 측정 → Capacity 추정 → 병렬도 계산 → 독립 부하 검증 → 운영 기준 도출입니다.

블로그에 첨부할 실측 근거는 [실험 증거 보고서](docs/experiment-report.md)에 정리했습니다. 실험 1~5의 관측·한계, Hot Key/Source 할당 표와 그림, 바로 인용할 설명, Run/Job ID·원본 검증용 CSV/JSON을 제공합니다. 최대 Capacity와 운영 병렬도의 Full 검증은 아직 Not Executed입니다.

실행 화면을 블로그에 첨부할 때는 [Web UI 캡처 안내](docs/ui-capture-guide.md)를 참고하세요. 캡처용 별도 Job의 접속 링크, 추천 화면·캡션, 실제 Source/Hot Key 관측, 재실행·중지 명령을 제공합니다.

현재 **Phase 0~5** 구현을 제공합니다. 실제 Kafka/Flink 계통·계측·격리와 실험1~4를 재사용해 실험5의 Source P/실제 Partition 할당 비교, 전체 순차 실행 CLI, 실측 CSV·PNG·한국어 통합 리포트를 추가했습니다. 장시간 Full의 실제 Capacity/계산값/병목 개선 검증은 **Not Executed**입니다. 구현과 실제 검증 범위는 [validation](docs/validation.md), 전체 계약은 [benchmark-spec](docs/benchmark-spec.md), 단계 이력은 [implementation-plan](docs/implementation-plan.md)에 있습니다.

```text
Python Producer → Kafka KRaft → Source → JSON Parse → Heavy CPU
                                          → keyBy(userId) → Keyed State → Measurement Sink
Python Smoke Controller → 실제 Job ID / Flink REST / 완료·오류·Checkpoint 계측
```

각 Operator의 P는 독립적이며 입력 1건에 출력 1건입니다. Sink는 외부 DB I/O 없이 완료 Counter와 Latency Histogram, Checksum을 기록합니다. 기본 Chaining은 꺼져 있고, Kafka Partition 선택은 userId와 독립적인 round-robin입니다.

## 기술 스택과 환경

- Flink `2.2.1-scala_2.12-java17`, Kafka Connector `5.0.0-2.2`, Kafka `4.2.1` KRaft
- Java 17 / Maven `3.9.11` 컨테이너 빌드, Python `3.12.12` / confluent-kafka `2.12.2` Producer
- Docker Engine와 Compose v2, 호스트 Python 3.10 이상(Controller는 표준 라이브러리만 사용), 선택 GNU Make
- 기본 자원: TM 2 CPU/2560MiB/16 Slots, JM 0.5 CPU/1536MiB, Kafka 1 CPU/1536MiB, Producer 0.5 CPU/512MiB. 최소 4 CPU와 약 8GiB 가용 RAM을 권장합니다. 빌드는 실험과 동시에 실행하지 않습니다.

버전·자원·State/Clock/계측 선택 이유는 [architecture](docs/architecture.md)에 있습니다. 호스트에 Java/Maven/Kafka Python 패키지를 설치할 필요가 없습니다. Docker 소켓 접근과 최초 이미지/Maven/PyPI 다운로드 네트워크가 필요합니다.

## 빌드와 Docker 실행

저장소 루트에서 실행합니다.

```bash
docker compose config --quiet
docker compose run --rm builder -B -ntp verify
docker compose build producer
python3 -m unittest discover -s tests -v
docker compose up -d --wait --wait-timeout 180 kafka jobmanager taskmanager
docker compose run --rm topic-init
```

`topic-init`은 기본 `lab-events` Topic을 32 Partition으로 생성합니다. 이미 존재하는 Topic의 Partition 수를 변경하지 않습니다. Smoke는 이 기본 Topic을 재사용하지 않고 새로운 Topic을 생성합니다. 볼륨 초기화 서비스는 Kafka UID1000/Flink UID9999에 루트 디렉터리 소유권을 지정합니다. Web UI: <http://localhost:8081>. Kafka host 주소는 `localhost:9092`, 컨테이너 주소는 `kafka:29092`입니다.

## 실제 End-to-End Smoke

```bash
python3 scripts/smoke_test.py
```

Smoke는 실제 600건을 생성하고 Kafka ACK, 각 Operator의 실제 P(2/3/4/2/3), Sink의 전체 eventId 집합, 중복·누락, 정상 Checkpoint, Job 종료를 확인합니다. Producer/Job/Topic은 Run별로 격리됩니다. 테스트 성공은 `VALID + UNKNOWN`입니다. 짧은 실행으로 지속 처리 Capacity를 주장하지 않습니다. 설정 예:

```bash
python3 scripts/smoke_test.py --records 1000 --target-rate 200 --partitions 8
python3 scripts/smoke_test.py --state-backend rocksdb
```

최대 10,000건까지 전체 ID Bitset 검증을 지원합니다. 실패 Run도 저장합니다. 테스트는 서비스를 남기고 해당 Run의 Job/Producer를 종료하며 Topic을 삭제합니다. Topic을 조사하려면 `--keep-topic`을 사용합니다. Checkpoint 파일은 보존되고 새 Run에서 복원하지 않습니다.

GNU Make 사용 시 `make build producer-image unit up smoke`, 종료는 `make down`입니다.

## Phase 2: 계측된 단일 Run

빌드와 Producer 이미지 생성 후 다음 명령을 실행합니다. Controller가 서비스 준비, 별도 Topic의 Producer/Kafka Preflight, 신규 Job 제출, 실제 Source→Sink 준비 확인, Warm-up/Measurement/Drain, 상태 판정과 정리를 수행합니다. 다른 Job이 실행 중이면 고정 자원 비교를 위해 실행을 거부합니다.

```bash
python3 scripts/preflight.py --target-rate 200 --duration 10
python3 scripts/run_once.py --mode smoke
python3 scripts/run_once.py --mode smoke --distribution skewed --seed 43
```

기본 Smoke는 Preflight 10초, Warm-up 5초, Measurement 20초, Drain 최대 60초, 수집 목표 간격 2초입니다. 기본 일반 설정은 60/180초, 수집 5초이며 [phase2.json](configs/experiments/phase2.json)에 있습니다. `--target-rate`, `--warmup`, `--measurement`, `--drain-timeout`, `--output-dir`, `--config`로 제어합니다. JSON 설정은 부분 병합하며 Operator별 P, CPU 작업량, State/Checkpoint, 분포, Partition 전략, 판정 임계값을 설정할 수 있습니다. 현재 Controller는 단일 Producer worker, fixed-resource, Chaining off를 지원하고 다른 모드는 명시적으로 거부합니다.

`--mode full`은 단일 Run의 60/180초 계측용 옵션이며 전체 5개 실험이나 반복 측정을 실행하지 않습니다. 이번 검증에서는 실행하지 않았습니다. Smoke는 `VALID + UNKNOWN`이 정상이며 Sustainable Capacity를 확정하지 않습니다. 유효한 Backlog 증가가 확인되면 `VALID + UNSTABLE`을 보존합니다. `INVALID`/`ERROR`는 종료 코드 1, `INCONCLUSIVE`는 종료 코드 0이므로 반드시 summary의 상태도 확인합니다. 판단 기준과 계측 정의는 [measurement-methodology](docs/measurement-methodology.md)를 따릅니다.

## Flink Job 직접 제출

직접 실행은 동일 Run 식별자를 재사용하지 않습니다. 다음은 신규 예시 Run입니다. 이전 실행이 있으면 Topic/Group/Run 이름을 함께 변경합니다.

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --topic manual-001 \
  --partitions 32 --replication-factor 1
python3 scripts/submit_job.py -- \
  --topic manual-001 --group-id manual-group-001 \
  --experiment-id manual --run-id manual001 \
  --source-parallelism 2 --parse-parallelism 2 --heavy-parallelism 4 \
  --aggregation-parallelism 2 --sink-parallelism 2 \
  --cpu-work-iterations 5000 --keyed-cpu-work-iterations 3000 \
  --max-parallelism 128 --chaining false --state-backend hashmap \
  --checkpoint-interval-ms 10000 --checkpoint-timeout-ms 60000 \
  --latency-sample-every 100
docker compose run --rm producer \
  --topic manual-001 --experiment-id manual --run-id manual001 \
  --target-rate 200 --duration 10 --kafka-partitions 32 \
  --distribution uniform --seed 42 --payload-size 256 \
  --output /results/manual001-producer.json
```

제출 CLI가 실제 Job ID를 출력합니다. Sink 완료 수는 Web UI 또는 해당 Job의 Subtask metrics에서 `completedRecords`를 확인합니다. 다른 Run의 이벤트/잘못된 JSON은 Parse 오류로 Job이 실패합니다. `--latency-sample-every 0`은 Latency/Heavy 계산 wall-time Histogram을 끕니다. 별도의 `--operator-cpu-sample-every 10`은 Heavy/Keyed 동기 본문의 Thread CPU 샘플을 켜며 기본0입니다. `--checkpoint-interval-ms 0`은 Checkpoint를 끕니다. `--state-backend rocksdb` 선택도 가능합니다.

직접 제출한 Job의 종료는 실제 ID로 실행합니다.

```bash
docker compose exec -T jobmanager /opt/flink/bin/flink cancel ACTUAL_JOB_ID
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --delete --topic manual-001
docker compose down
```

## 결과와 재현

```text
results/smoke/고유_Run_ID/
  metadata.json       # Host, Docker 제한, 실제 Graph/P, Topic/Group/Job, Clock 오차, 설정
  producer.json       # Scheduled/Sent/ACK, 오류, Partition 분포, 생성 Hot Ratio
  producer.log
  subtasks.json       # 실제 Subtask metric 이름/원시 값, 배치 정보
  checkpoints.json
  summary.json        # 정확성/종료 검증과 독립 상태
  job-exceptions.json # 실패 시 가능하면 저장
```

결과는 덮어쓰지 않습니다. `results/`는 Git에서 제외하지만 디스크에는 실패 결과도 남습니다. 공유할 때 해당 Run 폴더와 Git SHA, Compose 설정, 이미지 Digest를 함께 보존합니다. 아직 최초 커밋이 없으면 SHA는 null입니다. Raw Snapshot은 성능 비교용 5초 시계열을 대체하지 않습니다. 값이 없는 메트릭을 0으로 대체하지 않습니다.

Phase 2 저장 구조:

```text
results/preflight/preflight_UUID/   # Flink 실행 전 별도 발행 검증
results/phase2/run_UUID/
  metadata.json                    # 설정·환경·실제 Job/Graph·Clock·정리 결과
  producer.json                    # 구간별 Scheduled/Generated/Sent/ACK·분포·실제 런타임
  producer.jsonl                   # Producer Rate·Queue/Retry/Latency·cgroup 원시값
  kafka.jsonl                      # Producer 내 Admin 관측
  kafka_queries.jsonl              # REST 수집 후 새로 조회한 Partition LEO/Commit
  kafka_probe_diagnostics.jsonl    # Probe 응답 오류/stderr 원문과 실제 재시도 (발생 시)
  raw_samples.jsonl               # REST·Subtask·Histogram·cgroup 전체 원시 관측
  samples.csv                     # Job 시계열 (결측은 빈 셀)
  subtasks.csv                    # Operator/Subtask/배치/metric/value/status/reason
  lifecycle.jsonl                 # Ready/Warm-up/Measurement/Drain/종료 이력
  summary.json                   # 독립 상태·근거·회귀·완료율·적용 임계값
```

Measurement 생성 코호트와 Measurement 시간 내 완료를 구분합니다. Drain 완료를 측정 처리량에 합산하지 않습니다. Kafka read backlog와 committed lag를 별도로 기록하고, 신선하지 않은 offset은 null로 남깁니다. Histogram Bucket을 병합해 Job 퍼센타일을 계산하며 미처리 코호트가 남으면 Latency 불완전성을 표시합니다.

## Phase 3: 실험 1·2 실행과 분석

분석 의존성은 Python3.12.12/NumPy2.2.6/Pandas2.2.3/Matplotlib3.10.3 컨테이너에 고정했습니다. 호스트 Pandas를 설치할 필요가 없습니다. 이미지 빌드와 Unit Test는 부하 측정 전에 수행합니다.

```bash
docker compose build analyzer
docker compose run --rm --entrypoint python analyzer -m unittest discover -s tests -v
python3 scripts/run_experiment.py --experiment 1 --mode smoke --dry-run
python3 scripts/run_experiment.py --experiment 1 --mode smoke
python3 scripts/run_experiment.py --experiment 2 --mode smoke
docker compose run --rm analyzer --experiment 1
docker compose run --rm analyzer --experiment 2
```

기본 Smoke는 각 실험 4개 Run, P=1/2, Partition32, Rate100, Warm-up3초/Measurement10초/Preflight5초입니다. Setup·Kafka CLI·Job 제출/정리 시간이 추가됩니다. Heavy20000/Keyed1000은 Calibration의 병목 후보를 관측하기 위한 동일 Workload이며 실제 Heavy 병목을 가정하지 않습니다. 모든 Run은 동일한 물리 예산을 사용하고 순차 실행합니다.

Smoke의 지속 안정성이 UNKNOWN이므로 실험 1 탐색 하한은 결측, 실험 2의 예측 P도 결측으로 남습니다. 대신 실험 2는 사전 지정 P=1/2의 **독립 Peak 부하 probe**로 검증 실행 경로를 확인합니다. 이를 계산한 P의 실제 검증이라고 표시하지 않습니다. 실제 계산값 주변 실행은 Full의 근거를 만족한 경우에만 시작합니다.

Full 실행 절차 (이번 작업에서 실행하지 않음):

```bash
python3 scripts/run_experiment.py --experiment 1 --mode full --dry-run
python3 scripts/run_experiment.py --experiment 2 --mode full --dry-run
# 소요 시간/자원과 설정을 검토한 뒤 명시적으로 실행
python3 scripts/run_experiment.py --experiment 1 --mode full
python3 scripts/run_experiment.py --experiment 2 --mode full
```

Full은 조건당 최소3회, 60/180초, 실험1 P=1/2/4/8/16, 실험2 Calibration P=2/4/8입니다. 실험별 Calibration은 신규 Run으로 수행합니다. 최고8개 Rate 시도/P와 범위 축소를 포함해 장시간 걸립니다. `--dry-run`의 상한 비용은 Load/Drain만 포함하며 Setup/API 시간은 제외합니다. `--parallelisms`, `--repetitions`, `--initial-rate`, `--max-rate`, `--max-attempts`, `--target-rate`(실험1 고정 부하), `--peak-input-rate`, `--target-utilization`, `--random-order`, `--config`를 지원합니다. 설정 파일은 [phase3.json](configs/experiments/phase3.json)이며 Smoke/Full 계획과 공통 Run 설정을 분리합니다.

공식만 계산하는 예 (설명용 입력이며 실측 Capacity가 아님):

```bash
python3 scripts/calculate_parallelism.py \
  --peak-input-rate 40000 --subtask-capacity 7000 --target-utilization 0.7 --operator heavy
```

Session 출력:

```text
results/experiment_N/session_UUID/
  session.json          # 실행 전에 고정한 조건/계획
  runs/run_UUID/        # Phase 2 전체 Raw·CSV·메타데이터·정리 결과
  runs.jsonl            # 개별 Run/역할/반복/실제 상태
  search.jsonl          # 모든 Rate 시도·판정
  diagnostics.json      # 실제 Operator 처리율/포화 후보/자원 근거
  prediction.json       # 실험2: 검증 시작 전에 고정한 예측 또는 거부 이유
  summary.csv
  session_summary.json  # 탐색 범위·예측·관측 최소·추천·오류
  analysis_UUID/        # 새 분석마다 별도 경로 (덮어쓰기 방지)
    summary.csv / repeat_statistics.csv / operator_diagnostics.csv
    capacity_estimation.csv / latency_trend.csv
    *.png / report.md / analysis.json
```

분석 CLI는 기본적으로 해당 실험의 가장 최근 완료 **Smoke** Session을 읽습니다. Full은 `--mode full`로 선택합니다. 특정 Run 집합은 `--session results/experiment_N/session_UUID`로 지정합니다. INVALID/ERROR도 개별 표에 보존하며 통계에서 제외하고, 다른 Rate·역할·Smoke/Full을 섞지 않습니다. 결측 차트 상태는 analysis.json에 저장합니다. Pandas가 설치된 환경에서는 `python3 scripts/analyze_results.py --experiment 1 --session ...`도 가능합니다. 계산 조건과 실패 기준은 [capacity-calculation](docs/capacity-calculation.md), 실험 정책은 [experiment-design](docs/experiment-design.md)를 따릅니다.

## Phase 4: Operator별 병목과 Data Skew

Java JAR와 Producer 이미지를 갱신한 뒤 실행합니다. 기본 Smoke는 각 실험4 Run으로, 짧은 저부하 계통/계측 검증입니다.

```bash
docker compose run --rm builder -B -ntp verify
docker compose build producer
make unit-analysis
python3 scripts/run_experiment.py --experiment 3 --mode smoke
python3 scripts/run_experiment.py --experiment 4 --mode smoke
docker compose run --rm analyzer --experiment 3
docker compose run --rm analyzer --experiment 4
```

`make phase4-smoke`로 두 실험을 순차 실행할 수 있습니다. 실험3은 Focused Graph의 저작업량 사전 점검 후 A=(2,2,2,2,2), B=(8,8,8,8,8), C=(2,2,8,4,2)를 같은 입력률/Workload에서 비교합니다. 사전 점검과 비교 결과를 분리합니다. 의도한 Heavy 병목은 실제 메트릭 후보로 확인해야 하며 없으면 해석을 보류합니다.

실험4는 Smoke P=2/4, Full P=2/4/8/16의 Uniform/Skewed 쌍을 생성합니다. Heavy0/Keyed200000 iterations로 Aggregation 작업을 높이되 실제 병목을 가정하지 않습니다. Kafka Partition은 userId와 독립적으로 배치하며 실제 ACK 분포·생성 Hot 비율·Sent/ACK Hot 수를 검증합니다. 생성/처리 코호트의 Hot 건수를 대조해 실제 담당 Subtask를 찾으며 예상 hash 배치를 결과로 출력하지 않습니다.

```bash
python3 scripts/run_experiment.py --experiment 4 --mode smoke --distribution skewed
python3 scripts/run_experiment.py --experiment 4 --mode smoke --parallelisms 2,4 --hot-key-ratio 0.4 --seed 43
python3 scripts/run_experiment.py --experiment 3 --mode full --dry-run
python3 scripts/run_experiment.py --experiment 4 --mode full --dry-run
```

단일 `--distribution`은 해당 분포만 관측하고 Uniform/Skewed 비교를 Not Executed로 표시합니다. 공통 옵션은 `--target-rate`, `--repetitions`, `--random-order`, `--config`, `--output-dir`이며 `--parallelisms`는 실험4에만 적용합니다. Full은 최소3회, 기본 60/180초이며 사전점검 포함 실험3=12 Run/실험4=24 Run입니다. 위 dry-run은 실제 부하를 실행하지 않습니다. 명시적으로 Full을 실행할 때만 `--dry-run`을 제거합니다.

설정은 [phase4.json](configs/experiments/phase4.json), 비교 전제·병목 후보 기준·CPU 샘플의 의미와 한계는 [phase4-design](docs/phase4-design.md)을 따릅니다. 미관측 병목에 맞춰 같은 Session의 Workload를 바꾸지 않습니다. 조정이 필요하면 이유를 기록한 새 설정/Session으로 전체 비교를 다시 실행합니다.

결과는 Phase 3와 같은 `results/experiment_N/session_UUID/`에 저장됩니다. 새 `analysis_UUID/`에는 개별/반복 Summary, Operator·Subtask CSV, comparisons/diagnostics/environments JSON, Throughput/Latency/자원 및 Subtask Heatmap·Hot 부하·Partition 실측 PNG와 한국어 report.md가 있습니다. 비교 전제가 실패하면 해당 공통 그래프를 생성하지 않고 사유를 남깁니다. CPU Time은 샘플링된 연산 구간 비용이며 전체 Subtask CPU 사용률이 아닙니다.

## Phase 5: Kafka Partition과 Source P·전체 실행

실험5는 Kafka8 Partition, Source P=2/4/8/16, Downstream의 모든 P=8, CPU/Keyed 작업0을 기본으로 비교합니다. 실제 SourceReader 할당 콜백을 계측하고 Count=0을 포함해 모든 Reader를 조회합니다. 읽기 기록이 없다는 이유만으로 미할당이라고 판단하지 않습니다. Downstream Busy·입력 정합·실제 할당을 별도 비교 전제로 판정합니다. [phase5-design](docs/phase5-design.md)에 적용 조건과 한계를 명시했습니다.

```bash
python3 scripts/run_experiment.py --experiment 5 --mode smoke --dry-run
python3 scripts/run_experiment.py --experiment 5 --mode smoke
python3 scripts/run_experiment.py --experiment 5 --mode smoke --target-rate 20
docker compose run --rm analyzer --experiment 5 --mode smoke
```

`--parallelisms`는 실험5에서 Source P만 바꾸며 Downstream을 바꾸지 않습니다. `--target-rate`, `--repetitions`, `--random-order`, `--config`를 지원합니다. 기본 Smoke는4 Run이며 Source Capacity를 추정하지 않습니다. Full 기본12 Run/3회/60·180초입니다. 미등록 pendingRecords를0으로 채우지 않고 Source별 원시값과 전체 합계의 결측을 저장합니다. Source Assignment Adapter는 단일 고정 Topic/Unbounded/Discovery off/Restart0 조건에 한정됩니다. 기본 noWatermarks이며 Event-time/Idle Watermark는 별도 검증이 필요합니다.

전체1~5는 순차 실행합니다. 기본 all Smoke는20 Run입니다. Setup/API/정리 시간은 dry-run 부하 시간에 추가됩니다. 명시적 축소 설정은14 Run이며, 실험1·2는 P1 실행 경로, 실험3은 사전점검+3 Case, 실험4는 P2 두 분포, 실험5는 P2/4/8/16 모두를 확인합니다.

```bash
python3 scripts/run_experiment.py --experiment all --mode smoke --dry-run
python3 scripts/run_experiment.py --experiment all --mode smoke --config configs/experiments/suite-quick-smoke.json
# CLI가 출력한 실제 suite_directory를 사용 (아래 경로는 형식 예시)
docker compose run --rm analyzer --experiment all --mode smoke --suite results/suites/suite_UUID
# 이전 실제 결과를 실험별 최신 Smoke Session으로 다시 분석
docker compose run --rm analyzer --experiment all --mode smoke
```

Suite는 frozen 전체 설정/순서/Artifact Hash와 실제 Session 경로를 보존합니다. 정리 실패·Session 오류·측정 중 코드/JAR 변경이면 다음 실험을 중단합니다. 정상 정리된 INVALID/저성능 Run은 그대로 보존하고 종료 상태에 반영합니다. all의 `--config`는 [suite-quick-smoke.json](configs/experiments/suite-quick-smoke.json) 형식의 실험별 config/overrides이며 개별 Phase 설정과 다릅니다. 모호한 전역 --target-rate/--parallelisms 대신 실험별 overrides를 지정합니다.

```text
results/suites/suite_UUID/
  suite.json / sessions.jsonl / suite_summary.json
results/experiment_5/session_UUID/analysis_UUID/
  summary.csv / repeat_statistics.csv / assignments.csv
  subtask_diagnostics.csv / metric_trends.csv
  diagnostics.json / environments.json / analysis.json / *.png / report.md
results/reports/report_UUID/
  summary.csv / report.json / blog-report.md
```

통합 한국어 리포트는 정확히 선택한 Session을 링크하고 Capacity 예측/독립 검증, 고정 물리 자원, Hot Key, Source Partition 제약을 중심으로 작성합니다. 실험/계측이 없는 결과는 Not Executed이고 Raw와 기존 리포트를 덮어쓰지 않습니다. 실험 간 Workload가 달라 하나의 Capacity 곡선으로 합치지 않습니다. 결과를 공유할 때 상대 링크가 유지되도록 results 트리를 함께 보관합니다.

이번 실제 검증의 전체 축소 Suite는14 Run 중11 VALID/2 INVALID/1 ERROR였습니다. 실패를 보존하고 수집/정리 오류를 수정한 뒤 실험5 Rate20 네 조건과 실험1 두 조건의 독립 Smoke를 재검증했습니다. 모두 VALID+UNKNOWN이며 전체 새 Suite의 성공과 구분합니다. 실측 링크와 한계는 [블로그 입구](docs/blog-report.md), 명령·Run ID·테스트 근거는 [검증 기록](docs/validation.md)을 확인하세요. Smoke를 Sustainable Capacity로 해석하지 않습니다.

`make phase5-smoke`, `make suite-smoke`, `make analyze-all`도 제공합니다. [reproducibility](docs/reproducibility.md)에 전체 재현·보존·정리 절차가 있습니다. Full 계획은 아래처럼 확인합니다. **이번 검증에서는 Full을 실행하지 않았습니다.** 장시간 실행을 명시적으로 선택한 경우에만 --dry-run을 제거합니다.

```bash
python3 scripts/run_experiment.py --experiment all --mode full --dry-run
```

## Troubleshooting

- Docker `permission denied`: `docker info`로 소켓 권한/Context를 확인하고 Docker Desktop WSL integration 또는 로컬 Docker 권한을 설정합니다.
- Maven 없음/Java 버전 차이: 호스트 `mvn` 대신 Compose builder를 사용합니다.
- Kafka AccessDenied/Checkpoint IOException: init 서비스가 성공했는지 `docker compose logs kafka-volume-init checkpoint-init`로 확인합니다. 기존 State 자료를 임의 삭제하지 않습니다.
- RUNNING 대기 Timeout: `docker compose logs --tail 100 jobmanager taskmanager`, Web UI 예외, REST `/overview`의 TM/Slot을 확인합니다. 기본 최대 P16보다 큰 P는 Slot 부족으로 대기할 수 있습니다.
- Kafka 외부 연결 오류: Host와 Container의 bootstrap 주소를 구분합니다. 명시한 Partition 수는 실제 Topic metadata와 일치해야 합니다.
- 포트 충돌: Compose port와 Controller `--rest-url`을 함께 변경합니다. Kafka 외부 포트 변경 시 EXTERNAL advertised listener도 변경합니다.
- 최초 다운로드 실패: 인터넷/프록시/Maven Central/PyPI/Docker Registry 접근을 확인합니다. 실행하지 못한 테스트를 성공이라고 표시하지 않습니다.
- Producer Rate 미달: Phase 2 summary의 producer_validation, producer.jsonl의 expired/queue_full/errors/Rate/CPU를 확인합니다. Preflight 실패 시 Job을 제출하지 않습니다. 실행 중 속도 미달도 INVALID로 보존합니다.
- Offset 결측: sourceMetricSnapshotTimestampMs, kafka_queries.jsonl 시각과 Missing 사유를 확인합니다. 계측 fetch 간격은 1초이며 실제 수집 시간이 목표 간격보다 길어질 수 있습니다. API 실패는 ERROR, 근거 부족은 INCONCLUSIVE입니다.
- Kafka Probe JSON 오류: kafka_probe_diagnostics.jsonl의 stdout/stderr·attempt·returncode를 확인합니다. 빈/잘못된 JSON만 kafka_probe_attempts(기본2) 한도에서 실제 조회를 재시도하며 실패를0으로 채우지 않습니다. 성공한 Producer 종료 race는 실제 마지막 snapshot과 Drain Admin 조회로 검증합니다.
- Smoke 실패: 반환 코드는 1이며 고유 Run 폴더의 summary/예외를 확인합니다. 실패 처리량을 임의 삭제·수정하지 않습니다.
- Source 할당 INCONCLUSIVE: assignments.csv의 모든 Reader timestamp/count/list, Kafka Partition 소유자 완전성, Restart/읽기 충돌을 확인합니다. Count 결측을0으로 보정하지 않습니다.
- 전체 Suite 중단: suite_summary.json의 errors와 마지막 Session/Run cleanup_errors를 확인합니다. 같은 Suite를 덮어써 재개하지 말고 실제 남은 자원을 정리한 뒤 새 Session으로 필요한 실험을 실행합니다.
- 그래프 Not Executed: analysis.json의 reason·실제 메트릭·비교 전제를 확인합니다. Smoke Capacity/예측 P가 null인 것은 설계된 판단이며 임의 예시값을 넣지 않습니다.
- 분석 경로 오류: 컨테이너에서는 results 상대 경로 또는 `/workspace/results/...`를 사용합니다. Full 최신 선택은 --mode full이며 Smoke와 섞지 않습니다.

## 기술적 한계

단일 Kafka broker, 로컬 Checkpoint, 한 TM, 외부 I/O 없는 Sink는 Production 토폴로지가 아닙니다. P는 실제 CPU와 다르며 16 Slots가 16 CPU를 의미하지 않습니다. CPU quota 합이 Host 용량과 같아도 경합·Throttle이 생길 수 있습니다. Busy Time은 CPU Usage가 아닙니다. Checkpoint EXACTLY_ONCE 모드가 Measurement Counter의 Exactly-once를 보장하지 않습니다. JIT/GC/State cardinality·Clock 오차·coarse Histogram·샘플링 비용을 고려해야 합니다. 상세 내용은 [limitations](docs/limitations.md)에 있습니다.
