# Phase 0~5 실제 검증 기록

검증일: 2026-10-09, WSL2/Linux x86_64. 아래 이전 Phase 기록은 당시의 상태를 보존한다. Phase 3/4는 실험 1~4의 짧은 Smoke만 실행했다. Full Benchmark와 Sustainable Capacity/계산한 병렬도/장시간 병목 개선의 실측 검증은 **Not Executed**다.

## Repository와 실행 환경

- 최초 상태: 사용자 코드·기존 파일 없음, main 브랜치 최초 커밋 없음. Git SHA는 null이며 커밋을 임의 생성하지 않았다.
- Host: Java21.0.12, Python3.14.4, Maven 명령 없음, CPU affinity 4.
- Docker: Engine29.1.2, Compose2.40.3-desktop.1, Docker Desktop/WSL2, 4 CPU, 메모리10,430,435,328 bytes. 초기 sandbox Docker socket/네트워크 접근은 제한됐으며 승인된 실행에서 실제 접근·다운로드·테스트를 수행했다.
- Builder 실제 런타임: Maven3.9.11, Temurin Java17.0.17.
- Flink 실제 런타임: 2.2.1 / Temurin Java17.0.20.1.
- Producer 실제 런타임: Python3.12.12 / confluent-kafka2.12.2 / librdkafka2.12.1.
- Kafka broker:4.2.1, Connector:5.0.0-2.2, Connector 전이 Kafka client:4.2.0. 실제 ACK/E2E로 확인했다.

같은 Java17이어도 builder와 runtime patch 버전은 다르며 각각 기록한다. CPU affinity와 Docker quota를 혼동하지 않는다. Run별 Docker inspect/Config/TaskManager 상세/Clock probe는 metadata.json에 있다.

## 실행한 명령과 결과

| 실제 명령 | 결과 |
|---|---|
| `docker info` | 승인 실행에서 실제 데몬 접근 성공 |
| `docker pull flink:2.2.1-scala_2.12-java17` | 성공 |
| `docker pull apache/kafka:4.2.1` | 성공 |
| `docker pull maven:3.9.11-eclipse-temurin-17` | 성공 |
| `docker compose config --quiet` | 최종 설정 검증 성공 |
| `docker compose run --rm builder -B -ntp verify` | 최종 BUILD SUCCESS, Java Unit5개, 실패0/오류0/Skip0 |
| `docker compose build producer` | 최종 Python3.12/고정 Kafka client 이미지 빌드 성공 |
| `python3 -m unittest discover -s tests -v` | Python Unit12개 통과 |
| `python3 -m compileall -q scripts load-generator tests` | 성공 |
| `docker compose up -d --wait --wait-timeout 180 kafka jobmanager taskmanager` | 권한 초기화 수정 후 Kafka/JM/TM healthy |
| `python3 scripts/smoke_test.py` | HashMap 실제 E2E 성공; 상세 Run 아래 |
| `python3 scripts/smoke_test.py --state-backend rocksdb` | 최종 JAR/Producer의 RocksDB 실제 E2E 성공 |
| `docker compose run --rm topic-init` | 실제 `lab-events` Topic 자동 생성 성공 |
| `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic lab-events` | 실제 PartitionCount32, ReplicationFactor1 확인 |
| `docker compose run --rm builder -B -ntp org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree -DoutputFile=target/dependency-tree.txt` | 전이 의존성 그래프 저장 성공 |

Java 테스트: 독립 계산 참조, Payload seed 구분, 파싱·외부 Run 거부, Key별 집계 및 Checkpoint Snapshot/Restore, 옵션 범위, Sink 완료/Checksum/Latency/중복/검증 범위. Python 테스트: Uniform/Skewed/독립 Partition, 입력 검증, Missing/정수 정밀도, 전체 eventId audit/교차 Subtask 중복, 덮어쓰기 방지, 긴 URL/동적 메트릭 발견, Fake Clock·ACK 생산, 정확한 Job/Producer 종료·정리 실패 보존. Fixture 테스트는 results/에 저장하지 않는다.

## 실제 HashMap Smoke

Run: `smoke_20261009T065345_e6c05aa7`

Job: `e60c967c5dc054be4e38a3fe0dcac7e4`

- 실제 Graph P: Source2 / Parse3 / Heavy4 / Aggregation2 / Sink3, maxP128, Chaining off.
- 실제 Kafka ACK600건, Partition8개 각각75건.
- Sink 완료600건, 유일 eventId600개, 중복0, 누락0, Parse 오류0.
- Sequence Sum179700; 전체 bounded ID 집합을 Subtask 간 병합 검증.
- 실제 Checkpoint11회 완료.
- Run별 Producer 컨테이너 제거, Job CANCELED 확인, Topic 삭제 완료 확인, cleanup_errors 없음.
- **VALID + UNKNOWN**. 지속 처리 Capacity 추정값 null. 짧은 Smoke이고 Phase2 안정성 판정을 구현하지 않았으므로 STABLE로 보고하지 않는다.
- Clock probe RTT311~386ms, 각 offset의 가능한 범위를 기록했다. 이 넓은 오차로 절대 Latency 정확성을 주장하지 않는다.
- Raw: `results/smoke/smoke_20261009T065345_e6c05aa7/`. 메타데이터/Producer/Subtask/Checkpoint/Summary를 보존했다.

## 실제 RocksDB Smoke 및 최종 정리

최종 Jackson BOM/JAR/Producer를 사용한 Run: `smoke_20261009T065828_120671d9`, 실제 Job ID `7198078dbb198899c08bbbd4bcb57cb1`.

Kafka ACK600건, 유일 Sink ID600개, 중복/누락/Parse 오류0, Checkpoint10회 완료. `VALID + UNKNOWN`, capacity_estimate=null. Producer 제거, Job CANCELED, Topic 삭제 완료 확인, cleanup_errors 없음. Raw는 `results/smoke/smoke_20261009T065828_120671d9/`에 보존했다.

TaskManager 로그에서 `EmbeddedRocksDBStateBackendFactory` 선택과 `Successfully loaded RocksDB native library`를 직접 확인했다. HashMap Run의 로그는 `HashMapStateBackend`를 사용했다.

실제 `/jobs/overview`에서 실패 Run2개와 성공 Run2개 모두 CANCELED임을 확인했다. Kafka/JM/TM 서비스는 Web UI 확인 및 후속 실행을 위해 유지했다. 종료 명령은 `docker compose down`이며 named volume/Raw 결과를 자동 삭제하지 않는다.

추가 관측: Flink TM의 기본 논리 CPU resource profile은 Slot 설정으로부터16.0으로 보고되었으나 Docker NanoCpus 한도는2 CPU이다. 이 논리값을 실제 CPU 할당으로 해석하지 않는다. Phase2의 CPU 지표는 cgroup quota/Throttle/Process 사용량을 기준으로 별도 수집한다.

## 실패와 수정

| 실제 실패 | 확인 및 수정 | 재검증 |
|---|---|---|
| 첫 Maven compile: `setCheckpointStorage` API 없음 | Flink2.2에서 제거된 setter 대신 execution.checkpointing.storage/dir 설정 | 최종 verify 성공 |
| Kafka bootstrap AccessDenied | named volume root UID 불일치. Kafka UID1000/Flink UID9999 초기화 서비스 | Kafka healthy 및 실제 Checkpoint |
| Smoke1 REST404 /bad-request | Source80개 메트릭 요청의 URL이 5105~5328문자, 기본 요청 길이 초과. 3000문자 이하로 분할 | 긴 URL 회귀 Unit/실제 Smoke 통과 |
| Smoke2 parseErrors missing | 비동기 등록 중 첫 메트릭 목록을 캐시하여 신규 Counter 누락. 목록 재조회·Missing 대기 | 동적 등록 회귀 Unit/실제 Smoke 통과 |
| Jackson 전이2.18.2와 직접2.18.3 혼합 | BOM2.18.3으로 통일, Shade private namespace 유지 | 최종 Maven verify 성공 |

실패 Run `smoke_20261009T064622_976b6316`, `smoke_20261009T065028_8edd1d7f`는 `ERROR + UNKNOWN`과 예외/생산/메타데이터를 보존했다. 두 Run도 Job CANCELED·Topic 삭제 요청 성공. 그 결과를 성공 Run으로 수정하지 않았다. 초기 버전의 실패 스냅샷 저장 한계는 최종 코드에서 개선했다.

Maven Shade의 LICENSE/NOTICE/MANIFEST 중복 리소스 경고는 남아 있다. 계산/서비스 클래스의 중복 오류는 관측되지 않았고 실제 E2E로 실행을 확인했다. 일부 Configuration.setString 사용은 deprecated API 경고를 낼 수 있으며 현재2.2.1에 컴파일된다.

## pull한 이미지 Digest

```text
flink:2.2.1-scala_2.12-java17
sha256:140c3909f06cbea741be78fc630e809fae9dadcbfa492a684acf39efbbf68946
apache/kafka:4.2.1
sha256:9916d60eca5d599550e2c320230808fda342124ba550bb4ac4ea8591803262a0
maven:3.9.11-eclipse-temurin-17
sha256:e4a7ace3dc0d645ed97f8d9ad0b0d3f0b14fa8d150138f27f116d7105a639b82
python:3.12.12-slim-bookworm
sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
```

## 완료 판정

**Phase0 완료**: Repository/환경 조사, 공식 버전 존재 확인, 실제 호환 빌드와 실행, 전체 명세/계획/architecture/AGENTS, 자원·계측 계획 작성.

**Phase1 완료**: Compose 검증, 실제 Kafka/Flink readiness, Java17 build, Operator별 P/CPU 강도, 기본 Producer 실제 ACK, Sink 정확성, 핵심 Unit, 실제 E2E·Checkpoint·종료 검증 충족.

위 기록은 Phase 1 종료 당시의 검증이다. 당시 미구현이던 Preflight·주기 Lag·구간 자동화·Sustainable 판정·CSV·CPU/Throttle/GC는 아래 Phase 2에서 구현했다. 5가지 실험, Capacity 공식/독립 검증, 반복 Summary/PNG/블로그 자동 리포트, 정확한 Subtask CPU 귀속과 장애 재시작 처리 보장은 여전히 완료하지 않았다.

전체 성능 실험 결과는 blog-report.md의 **Not Executed** 상태를 유지한다.

## Phase 2 구현과 실제 검증 (2026-10-09)

기존 명세/계획/코드/검증 기록을 확인하고 Phase 2에 집중했다. Phase 3~5 실행기·Capacity 계산·차트는 구현하지 않았다. Docker 소켓 접근이 필요한 실행은 승인된 환경에서 수행했다. 기존 Raw/Summary는 변경하지 않았다.

| 실행한 명령 | 실제 결과 |
|---|---|
| `docker compose run --rm builder -B -ntp verify` | 최신 JAR BUILD SUCCESS, Java 5개 테스트 실패/오류/Skip 0; Sink 구간 경계 테스트 확장 |
| `docker compose build producer` | session/Admin/cgroup 포함 Phase 2 이미지 성공; 마지막 polling timeout 보정도 빌드 성공 |
| `docker compose config --quiet` | 성공 |
| `python3 -m unittest discover -s tests -v` | 최종 Python 34개 통과 (Phase 1 회귀 12개 + Phase 2 22개), 실패0 |
| `python3 -m compileall -q scripts load-generator tests` | 성공 |
| `python3 scripts/smoke_test.py` | Phase 1 회귀 E2E 성공, 600 ACK/유일 ID, 중복/누락/Parse 오류 0, Checkpoint 5회, 정리 성공 |
| `python3 scripts/run_once.py --mode smoke` | 수정 후 단일 Run 자동화 VALID + UNKNOWN |
| `python3 scripts/run_once.py --mode smoke --distribution skewed --seed 43` | Skewed 단일 Run 계측 VALID + UNKNOWN; 실험 4 비교가 아님 |

Phase 1 회귀 Run은 `smoke_20261009T075517_65fe37ce`, Job은 `85952fe0626007f073303287f65eee94`. 소스 코드의 이전 “Phase 2 미구현” 메시지는 Phase 1 Smoke가 새 판정기를 호출하지 않는다는 설명으로 수정했다. 저장된 과거 결과 문구는 그대로 보존한다.

### Uniform 자동 계측 Run

Run `run_7d18a9e25b4447ab8bef1401567f6068`, 실제 Job `7bd7c4cefc3456e83b1ddef7270aefc1`. Raw 경로는 `results/phase2/run_7d18a9e25b4447ab8bef1401567f6068/`이다. 독립 Preflight `preflight_7c702abcec334142bae41989931d83dd`는 유효 판정을 받았다.

- 입력 200 records/sec, Warm-up 5초/Measurement 20초, 신규 Partition 8개, Operator P 각각2, TM quota2 CPU.
- 전체 준비8 + Warm-up1000 + Measurement4000 = 5008 ACK/완료/유일 ID. 중복·누락0.
- Measurement 코호트4000 중 시간 내3937, Drain63, 최종 미처리0. 코호트 시간 내 처리율196.85 records/sec이며 Capacity 추정값이 아니다.
- 최종 Source read backlog0, 별도의 committed lag113. 두 값을 같은 Lag로 취급하지 않았다.
- Measurement sample 범위 Checkpoint20회 완료/실패0; 누적 실패2는 시작 단계 관측이다.
- Measurement sample10개 중 신선한 backlog 회귀 표본9개. slope 약0.748 records/sec, 정규 근사95% 구간[-4.633,6.130], 누적 변화15. 이 짧고 노이즈가 있는 자료로 STABLE을 주장하지 않았다.
- 실제 cluster occupied Slots2. Operator P 합10을 CPU/Slot 개수로 사용하지 않았다.
- Python3.12.12/confluent-kafka2.12.2/librdkafka2.12.1 실제 런타임 저장.
- Producer 제거, 실제 Job CANCELED, Topic 삭제 확인, cleanup_errors 없음. 결과 VALID + UNKNOWN, sustainable_throughput=null.

### Skewed 계측 확인

Run `run_e1046e7c37bc4bdf913aa826c90f348e`: 전체5008 ACK/유일 완료, Measurement4000 중 시간 내3965/Drain35, 최종 read backlog0, Measurement Checkpoint19회 완료/실패0, 필수 결측없음. Producer cgroup delta도 summary에 집계됐다. VALID + UNKNOWN이며 Job/Topic/컨테이너 정리가 완료됐다. 이 Smoke 중 Producer 이미지의 작은 재빌드가 겹쳐 리소스 성능 비교에는 사용하지 않는다. 입력 분포·구간·정리 동작 확인 범위로만 해석한다. 최신 이미지 재검증 Run은 아래 추가 기록을 따른다.

### 발견한 문제와 수정

| 관측 | 수정/해석 |
|---|---|
| 초기 Run의 REST 캐시가 10초 단위로 offset/Histogram을 갱신 | fetch interval 1초, Subtask timestamp Gauge, 신선도 검사, REST 뒤 새 Admin 조회; 음수/오래된 차이는 null |
| Source/Sink 신선도 Gauge가 Subtask 한 개에만 나타남 | 캡처 없는 메서드 참조가 Gauge 객체를 공유함. Subtask마다 별도 Gauge 객체를 생성해 실제 재검증 |
| Startup Checkpoint 실패를 Measurement 실패로 분류 | Measurement sample 사이 Counter delta와 누적 총계를 분리; 회귀 Unit 추가 |
| Producer 종료 뒤 저장된 Admin snapshot이 Drain에서 오래됨 | 동일 이미지의 일회성 observer로 새 LEO/Commit 조회하고 정리 |
| Queue/구간 종료 예정 슬롯, API 실패/결측 구분 필요 | 미달은 INVALID, 실제 Backlog 증가 UNSTABLE 보존, API 실패 ERROR, 증거 부족 INCONCLUSIVE |
| polling 직전 시간이 경계를 넘을 수 있음 | poll timeout을 0 이상으로 제한; 최종 이미지 빌드 |

초기 `run_2c2bf06ceba0453298c0c416debb55ca`는 당시 코드에서 VALID + UNSTABLE로 저장됐다. 전체5008 유일 완료는 확인했지만, 오래된 메트릭과 Startup Checkpoint 실패를 섞은 초기 계측 판정이므로 Capacity/비교 근거로 사용하지 않는다. summary를 성공 결과로 고치지 않았으며 오류 발견 이력으로 보존한다.

`run_687f209839dc43d399ce5f7d04b00b67`는 Producer expired10건으로 INVALID + UNKNOWN, Measurement Sent/ACK/최종 완료3990건이다. Gauge 충돌로 신선도 근거도 부족했다. 이 Run 역시 Raw와 정리 결과를 보존한다. 실제 부족한 성능·유효성 실패를 프로그램 성공으로 변경하지 않았다.

### Phase 2 완료 조건과 검증 한계

Target/Actual/ACK 비교, 실제 Job ID/Subtask 수집, CSV/JSON 및 Missing 사유, 구간/Drain 분리, 별도 Preflight, Run별 Topic/Group/State 격리, 상태/결측/정리/덮어쓰기 방지 테스트, 실제 단일 자동 Run 성공을 확인했다. Phase 2 기능 완료 기준을 충족한다.

장시간 Full의 STABLE 판정은 Fixture Unit으로 검증했으며 실제 Full/Capacity를 측정하지 않았다. 고부하 발행, 반복 실험, 계측 오버헤드 정량 비교, Chaining on 진단, 다중 worker/resource-scaled, cgroup v1, 정확한 Subtask CPU/State cardinality, Prometheus reporter는 미검증 또는 미구현이다. 일부 신선도 한도 초과 샘플은 null로 보존했고 회귀에서 제외했다. Kafka CPU 포화/Throttle 경고를 보존했으며 이를 Flink Capacity 한계로 단정하지 않는다. Phase 3는 이 수집/판정기를 재사용해 Capacity 탐색과 계산값의 독립 검증을 구현해야 한다.

### 최종 이미지 재검증

실제 명령: `python3 scripts/run_once.py --mode smoke --target-rate 100 --warmup 3 --measurement 10 --seed 44`.

Run `run_b1279adedce2480188b0936ae1507ebb`, 실제 Job `c2ff6d4ae073dcaa408d3fe6d410e0d9`, 독립 Preflight `preflight_d97475578e7348c3a46f231c1678ef4e` 유효. Producer 이미지 ID는 `sha256:121d0611db2be3ade5c4691f929ae1b0e2066be0302ead1bec8fb9a3e124d380`이다.

- 전체 준비8 + Warm-up300 + Measurement1000 = 1308 ACK/유일 완료. 중복·누락0, 필수 결측없음.
- 실제 Measurement Sent/코호트 ACK Rate100.0 records/sec. Partition8개 각각125 ACK.
- Measurement1000 중 시간 내979, Drain21, 최종 미처리0. 측정 시간 내 처리율97.9 records/sec, 최대 Capacity가 아니다.
- 최종 read backlog0, 별도의 committed lag98. 측정 sample 범위 Checkpoint8회 완료/실패0, 시작 단계 누적 실패2 별도 보존.
- samples.csv 데이터9행, subtasks.csv 데이터12253행 생성. Raw와 JSON summary/metadata/생산 계측 저장.
- VALID + UNKNOWN, sustainable_throughput=null. Producer/Probe/Observer 정리 오류없음, 실제 Job CANCELED, Topic 삭제 확인. 마지막 REST 확인에서 활성 Job0개, Kafka/JM/TM healthy.
- Kafka/Producer Throttle 경고를 실제 관측해 보존했다. 짧은 저부하 계통 검증이며 리소스 포화의 원인이나 Flink Capacity를 확정하지 않는다.

최종 Python34개 Unit/compileall 통과. Full 안정성 판정은 노이즈가 큰 Latency의 회귀 상한도 확인하며 불확실하면 INCONCLUSIVE로 남긴다. 해당 경로는 추가 Fixture로 검증했다. 마지막 REST 조회는 sandbox의 로컬 socket 제한으로 한 번 차단됐고, 승인된 실행에서 활성 Job0개를 직접 확인했다. 서비스를 종료하려면 `docker compose down`을 사용하며 측정 Raw/Checkpoint 볼륨은 자동 삭제하지 않는다.

## Phase 3 구현과 실제 검증 (2026-10-09)

Phase 2 실행기·생산 계측·REST/Admin 수집·구간·판정·정리를 재사용했다. 실험 1의 Rate 탐색/고정 입력률, 실험 2의 조건부 Capacity 추정/계산/독립 검증 실행 경로, 반복 통계/실측 차트/한국어 리포트를 추가했다. 실험 3~5 실행기는 구현하지 않았다.

### 실제 Build와 Test

| 실제 명령 | 결과 |
|---|---|
| `docker compose config --quiet` | 성공 |
| `docker compose run --rm builder -B -ntp verify` | BUILD SUCCESS, Java 5개 실패/오류/Skip 0 |
| `docker compose build analyzer` | 고정 버전 분석 이미지 빌드 성공 |
| `python3 -m unittest discover -s tests -v` | 57개 중 55개 성공, 분석 의존성이 없는 Host에서 Pandas 관련 2개 Skip |
| `docker compose run --rm --entrypoint python analyzer -m unittest discover -s tests -v` | Python 57개 모두 성공, Skip 0 (기존 34 + Phase 3 23) |
| `python3 -m compileall -q scripts load-generator tests` | 성공 |
| `python3 scripts/run_experiment.py --experiment 1 --mode full --dry-run` | 실행 없이 최대135 Run/Load·Drain 예산41850초 계획 확인 |
| `python3 scripts/run_experiment.py --experiment 2 --mode full --dry-run` | 실행 없이 최대87 Run/Load·Drain 예산26970초 계획 확인 |
| `python3 scripts/run_experiment.py --experiment 1 --mode smoke` | 4 Run, Session COMPLETED, 모두 VALID + UNKNOWN |
| `python3 scripts/run_experiment.py --experiment 2 --mode smoke` | 4 Run, Session COMPLETED, 모두 VALID + UNKNOWN |
| 아래 분석 명령 2개 | 실험별 CSV/JSON/한국어 report.md/실측 PNG6개 생성 |

추가 Unit은 Decimal ceil 경계/Headroom/Operator별 입력률/후보 P, Rate 증가·초기 과부하·범위 축소·검열·판정 충돌, Checkpoint 실패를 Capacity 상한으로 오해하지 않는 규칙, 직접 Operator Counter/Reset/Missing, 복수 P의 조건부 추정·자원 포화/환경 차이 거부, Prediction 저장 후 독립 ID/Seed 검증, 최소 Stable P와 운영 P 구분, 정리 실패 후 다음 Run 중단, 반복 통계/물리 예산 분리/덮어쓰기 방지를 검증했다. Full 적응형 탐색과 계산 P 주변 검증은 TemporaryDirectory의 명시적 Fixture로만 검증했으며 results/에 실측처럼 저장하지 않았다.

분석 이미지: Python3.12.12, numpy2.2.6, pandas2.2.3, matplotlib3.10.3 및 requirements의 전이 버전 고정. 실제 빌드 manifest는 `sha256:6878bdb639ad368d7d3bc35669a04efc745ec302272923c00521cc8a9683df80`이다.

### 실제 Smoke 조건과 개별 결과

Kafka Partition32, 모든 Operator P=1 또는2, Heavy iterations20000/Keyed iterations1000, Payload256 bytes, Uniform/round-robin, fixed-resource, Chaining off, HashMap/maxP128, Checkpoint1초. Preflight5초, Warm-up3초, Measurement10초, 조건당1회, 목표100 records/sec. 실제 Measurement Sent/ACK 코호트 Rate는 8개 Run 모두100.0이다. 이는 저부하 자동화 확인이며 Capacity 측정이 아니다.

실험1 Session: `results/experiment_1/session_fe6a62bec3b74f80a5e127f9ae33063f/`

| 역할 | P | Run ID | Measurement 코호트 시간 내 완료 | Drain 완료 | 측정 범위 Checkpoint 완료/실패 |
|---|---:|---|---:|---:|---|
| discovery | 1 | run_31801b43273c4086ad5eee917fd5ca80 | 980 | 20 | 8/0 |
| discovery | 2 | run_e6f00112834f45a8b13b995b74a86e3e | 983 | 17 | 4/0 |
| fixed_input | 1 | run_ee4dc92768734c47b36e31558050a2a7 | 1000 | 0 | 8/0 |
| fixed_input | 2 | run_278c649b2968422abf5744434fa1a887 | 973 | 27 | 8/0 |

고정 부하 측정 코호트 처리율은 P1=100.0/P2=97.3 records/sec로 관측됐다. P 증가 시 항상 성능이 증가한다는 가정으로 결과를 바꾸지 않았다. 10초·1회 및 Clock/Histogram/계측·경계 영향으로 이 차이의 원인이나 통계적 유의성을 확정할 수 없다. Drain27건을 Measurement 처리량에 더하지 않았다.

실험2 Session: `results/experiment_2/session_50da7d363f794fc184eb2eaf68756479/`

| 역할 | P | Run ID | Measurement 코호트 시간 내 완료 | Drain 완료 | 측정 범위 Checkpoint 완료/실패 |
|---|---:|---|---:|---:|---|
| calibration | 1 | run_25f09a057c5840f293be00e110555a34 | 983 | 17 | 8/0 |
| calibration | 2 | run_a6f4129b9975484bafbb05a27ed58ea8 | 974 | 26 | 8/0 |
| uncalibrated_peak_probe | 1 | run_e445d485f95e4ad5a1e51c7643cc006f | 974 | 26 | 7/0 |
| uncalibrated_peak_probe | 2 | run_2821c88941194fb39f6e414eb3515ca1 | 985 | 15 | 8/0 |

実験2는 새 Calibration2개 후 `prediction.json`을 먼저 저장하고, 다른 Run ID/Seed의 Peak probe2개를 실행했다. 신뢰할 STABLE/과부하 경계가 없으므로 Operator Capacity/P 예측/최소 Stable P/운영 P/계산 오차는 모두 null/NOT_ESTIMATED다. Peak probe를 계산된 P의 검증이라고 보고하지 않는다. 각 Session 안의 비교 fingerprint는 각각1종으로 같았으며 Experiment 간 추정치를 혼합하지 않았다. 실험2 metadata에는 JAR/Producer/전체 scripts 파일 hash도 기록했다.

8개 Run 모두 준비32 + Warm-up300 + Measurement1000 = **1332 ACK/완료/유일 eventId**, 중복·누락0, Measurement 코호트 최종 미처리0, 최종 Source read backlog0, Restart0, 필수 계측 결측없음. 각 Run의 실제 Job ID/Graph/Offset/Clock/cgroup/개별 Histogram은 metadata/summary/Raw를 따른다. committed lag는 read backlog와 별도로 남겼다. Kafka quota 포화 및 Producer/Kafka Throttle 경고를 보존했고 Flink의 병렬도 한계라고 단정하지 않았다. 일부 샘플의 신선도·범위 부족과 Startup Checkpoint 누적 실패는 별도 기록이다.

모두 최종 Job CANCELED, Producer/Observer/Probe 제거 및 Topic 삭제 확인, cleanup_errors=[] 및 Drain timeout=false. 최종 실제 REST `/jobs/overview`의 활성 Job0개, Kafka/JM/TM healthy를 확인했다. 로컬 REST는 Sandbox socket 차단 후 승인된 실행에서 재조회했다. 서비스를 후속 실행/Web UI용으로 유지했다.

### 실제 분석 산출물과 재현

```bash
docker compose run --rm analyzer --experiment 1 --session results/experiment_1/session_fe6a62bec3b74f80a5e127f9ae33063f
docker compose run --rm analyzer --experiment 2 --session results/experiment_2/session_50da7d363f794fc184eb2eaf68756479
```

최종 분석 경로:

- 실험1: 위 Session의 `analysis_796888fb99f44d1fadde01613c9fe0e7/`
- 실험2: 위 Session의 `analysis_90f36205eadb4d5e9f63c64fdbd0a56d/`

각각 summary.csv, repeat_statistics.csv, operator_diagnostics.csv, capacity_estimation.csv, latency_trend.csv, environments.json, analysis.json, report.md를 생성했다. 실측 PNG는 throughput/latency/cpu/busy_idle_backpressure/backlog_slope/latency_trend 각6개다. Sustainable Throughput 차트는 두 실험 모두 Not Executed, prediction_vs_observed는 실험2에서 Not Executed다. 가짜 점이나 0 대체를 넣지 않았다. 이전 분석 UUID도 보존하며 다시 실행하면 새 디렉터리를 만든다. 1회 반복의 표본 Std는 null, 반복 Run p99 평균은 전체 이벤트 p99가 아니라고 리포트에 명시했다.

### 기술적 문제와 수정

- 같은 target에서 Maven verify를 반복하자 Shade가 기존 shaded JAR을 입력으로 재사용하여 클래스 중복 경고가 증가했다. maven-jar-plugin의 `forceCreation=true`로 원본 JAR을 재생성하고 verify/실제 E2E를 재검증했다. 최종에는 MANIFEST/LICENSE/NOTICE 중복 리소스 경고만 남았다. 공식 근거: [Maven JAR Plugin forceCreation](https://maven.apache.org/plugins/maven-jar-plugin/jar-mojo.html).
- Host에는 Pandas/Matplotlib가 없어 분석 Unit2개가 Skip됐다. 고정 버전 Docker analyzer를 구현해 같은 Python57개를 Skip 없이 실행하고 실제 분석 생성까지 확인했다.
- 측정 조건/물리 예산/Artifact가 다른 자료는 Rate 탐색·반복 통계·Capacity 추정에 혼합하지 않도록 fingerprint 검사를 추가했다. 목표 입력률의 Stable 하한과 실제 관측 Sustainable 처리율 하한도 별도 필드로 분리했다.

### 완료 판정과 미검증

**Phase 3 기능 완료 조건 충족**: 실험1 Smoke 실행, 공식 Unit 통과, 실험2 Smoke 실행, 예측/관측·Calibration/Validation 분리, 표/그래프·한국어 리포트, Full 실행 절차/비용 계획을 확인했다. 전체 결과와 실패/불확실성을 보존하는 자동화 코드가 구현됐다.

**미검증**: 실제 Full STABLE/UNSTABLE 경계·지속 최대 Capacity·병목 Operator의 전이 가능한 Subtask Capacity·계산된 P 주변 장시간 테스트·최소 Stable/운영 P·계산 오차·0.6/0.7/0.8 실측 민감도·Knee Point. 해당 Full 분기는 Unit Fixture로 검증했을 뿐 실측 검증 완료로 표시하지 않는다. 임계값은 실험 초기 정책이며 Flink 표준이 아니다. TM2 CPU 고정 예산에서 복수 P가 경합하면 기본 엄격한 Capacity gate가 추정을 거부할 수 있다. 충분한 근거를 얻지 못했을 때 보류하는 것이 정상 동작이다.

Phase 4에는 Operator별 Scale-out 비교, 실제 Heavy/Keyed 병목 검증, Uniform/Skewed의 Kafka Partition 정합성·Hot Subtask/부하 불균형·Heatmap 및 실험3/4 Smoke를 구현한다. 이번 작업에서는 실행하지 않았다.

## Phase 4 구현과 실제 검증 (2026-10-09)

benchmark-spec/implementation-plan/AGENTS, 기존 Java/Producer/실행·분석 코드와 Phase0~3 검증 기록을 확인했다. 기존 전체 파일은 Git untracked이고 최초 Commit SHA가 없어 null을 유지했다. 기존 Raw/Session/분석과 실패 기록을 삭제하지 않았다. Phase4의 실험3/4를 실제 구현했으며 Phase5의 실험5/all은 미구현이다.

### Build와 테스트

| 실제 명령 | 실제 결과 |
|---|---|
| `docker compose config --quiet` | 성공 |
| `docker compose run --rm builder -B -ntp verify` | BUILD SUCCESS, Java8개 실패/오류/Skip0 (기존5+추가3) |
| `docker compose build producer` | Hot Sent/ACK 계측을 포함한 이미지 빌드 성공 |
| `python3 -m unittest discover -s tests -q` | 최종75개 중72개 성공/Skip3: Host 분석 의존성 없음 |
| `docker compose run --rm --entrypoint python analyzer -m unittest discover -s tests -q` | 최종Python75개 성공/Skip0 (기존57+추가18) |
| `python3 -m compileall -q scripts load-generator tests` | 성공 |
| `python3 scripts/run_experiment.py --experiment 3 --mode full --dry-run` | 실제 부하 없이12 Run/Load·Drain 상한3720초 계획 확인 |
| `python3 scripts/run_experiment.py --experiment 4 --mode full --dry-run` | 실제 부하 없이24 Run/Load·Drain 상한7440초 계획 확인 |
| `python3 scripts/smoke_test.py` | 기존 E2E 회귀600 ACK/완료/유일 ID, 중복·누락0, Checkpoint8회, 정리 성공 |
| `python3 scripts/run_experiment.py --experiment 3 --mode smoke` | 4 Run, Session COMPLETED, 모두 VALID + UNKNOWN |

새 Unit은 실제 코호트/Hot/window 경계·관측 후 배치 Gauge, CPU 로컬 순번 샘플/유효·잘못된 읽기/미지원, 전제 설정·Full 반복수, Counter Reset/Missing/평균0, Skew 후보/다중 Hot 오염/신선도·수량 대조, 생성 분포·Partition TV·실제 Graph/Seed/자원 불일치, 사전 점검 보류, Operator Case/동일 반복 Seed/단일 분포 보류/안전 정리, CSV·PNG·반복 통계/분석 새 디렉터리, 실제 cgroup 시각 경계 제외를 검증한다. 분포 Generator 및 Stateful 정확성/Snapshot Restore는 기존 테스트를 재사용했다. Fixture는 TemporaryDirectory에서만 사용하며 results/에 실측으로 저장하지 않았다.

회귀 E2E: `results/smoke/smoke_20261009T102936_497bcbe4/`, 실제 Job `771d0f5c95751158cfb7ed09b56af2e6`. VALID + UNKNOWN, Parse 오류0, Producer 제거/Job CANCELED/Topic 삭제 확인, cleanup_errors=[]이다. 새 Producer 이미지 실제 config ID: `sha256:d24d55b50298f9400c3b39a628100870ea9dd6d076fadfcf1339fde12c0cdcfd`이며 manifest list는 `sha256:bca80a6d4c4859814cd6575543b202540df96a0c4f4b9f50310b388b5eb1883a`이다. 기존 고정 분석 이미지/패키지는 재사용했다.

### 실험 3 실제 Smoke

Session: `results/experiment_3/session_1950c0073c0443d2ac71dbbc565a321f/`.

Kafka Partition32, 목표/실제 Measurement Sent 및 ACK 코호트 Rate100 records/sec, Warm-up3초/Measurement10초/1회, Payload256, Uniform/round-robin, fixed-resource, Chaining off, HashMap/maxP128, Checkpoint1초, CPU body 샘플 매10번째 로컬 레코드. 실제 Graph P는 Named Case 설정을 검증했고 precheck는 Focused P에 Heavy/Keyed CPU0의 독립 조건이다. 비교 Case는 Heavy200000/Keyed1000 iterations를 동일하게 유지했다.

| Case | Run ID | Job ID | 측정 코호트 시간 내/Drain | Measurement sample Checkpoint 완료/실패 |
|---|---|---|---|---|
| focused_transport_precheck | run_8929a6b821dd469084de195e45b9d3f0 | 65bbacc3d99ed8ad41b2a24484684f62 | 978/22 | 7/0 |
| low_baseline | run_e8aeb5f72ad24496bfb2d466665ceadc | ed84c14c28661c150e1ef51eaccb4790 | 978/22 | 8/0 |
| uniform_high | run_8f7917d0159f4b59a33adcab3e01c36f | d7846a84189a2b3169f3af4321b8454c | 993/7 | 5/0 |
| bottleneck_focused | run_b8ca166515524f3f80e1cfeddc425dc7 | 2a699f2b7ed5e35b03eaea49b96dc762 | 980/20 | 8/0 |

4 Run 모두1332 ACK/완료/유일 ID (준비32+Warm-up300+Measurement1000), 중복·누락0, 최종 read backlog0 및 코호트 미처리0, Restart0, 정리 오류없음. 비교 Case의 fingerprint는 같고 다른 작업량의 precheck를 혼합하지 않았다.

사전 점검은 Sink 코호트 비율0.978과 Backlog CI의 증가 배제 부족으로 FAILED_OR_INCONCLUSIVE였다. 실행 자체는 VALID + UNKNOWN으로 보존했다. 설정 기준을 바꾸지 않고 실제 Case를 탐색적 관측으로 실행했다. 세 Case의 측정 시간 내 Sink 코호트 처리율97.8/99.3/98.0 records/sec는 10초·1회의 관측이며 개선 효과/동일 지속 성능을 증명하지 않는다. 실제 Heavy/Keyed CPU body 샘플은 수집됐지만 Busy/Backpressure 기준의 유일 Heavy 병목 후보는 관측되지 않았다. 병목 중심 증설의 성능 가설을 성공으로 표시하지 않았다.

### 기술적 문제와 수정

Docker 소켓은 Sandbox에서 차단돼 승인된 환경에서 Maven/이미지/실제 E2E/Smoke를 수행했다. Host에는 분석 의존성이 없어3개 Skip됐으나 같은 테스트75개를 고정 분석 컨테이너에서 Skip 없이 재검증했다. Java 빌드의 기존 deprecated API 및 MANIFEST/LICENSE/NOTICE 중복 리소스 경고는 남아 있고 실제 실행을 확인했다.

코드 검토에서 REST sample의 wall_time이 cgroup 수집보다 먼저 기록되는 것을 확인했다. CPU 비용을 REST 시각으로만 경계 제한하면 끝 이후 cgroup snapshot이 섞일 수 있어, 실제 cgroup raw.wall_time을 Measurement 범위로 제한하고 CPU 관측 범위/평균 사용 코어/표본 수도 함께 제공하도록 보정했다. 경계 제외 Unit을 추가했다. 실험3 이후 수정했으며 기존 Raw/원래 Session diagnostics를 수정하지 않았다. 분석은 Raw에서 비용을 재계산해 새 analysis_UUID에 저장하고 분석 코드 SHA256과 보정 적용 사실을 analysis.json에 기록한다. 실험4는 처음부터 보정된 계산기를 사용한다.

Thread CPU 샘플은 전체 Subtask CPU 사용률이 아니며 동기 계산 구간의 ns/샘플 근사다. 높은 Busy/낮은 Downstream Backpressure도 단독 인과 증명이 아니다. Warm-up·짧은 Measurement와 통계/샘플링·메트릭의 갱신 특성은 실제 병목 판단에 한계가 있다. 엄격한 전제 실패나 병목 미관측은 결과 수정 사유가 아니다.

### 실험 4 실제 Smoke

실제 명령: `python3 scripts/run_experiment.py --experiment 4 --mode smoke`.

Session: `results/experiment_4/session_8a3381c7f9b04c4b97bb8b26bc0d9069/`.

Heavy0/Keyed200000 iterations, P=2/4의 Uniform/Skewed, Hot 설정0.4, 같은 Seed42. 기타 조건은 위 Smoke와 같고 CPU/메모리 예산을 늘리지 않았다. 각 Run의 입력 분포 전제는 VALID였으며 각 P의 Uniform/Skewed ACK Partition Total Variation은0.0(기준0.02)이었다. 실제 Graph/Seed와 분포 외 Workload·Artifact·물리 예산도 쌍 안에서 같았다.

| 분포/P | Run ID | 실제 Job ID | 시간 내/Drain | sample Checkpoint 완료/실패 | 코호트 처리 부하 Max/Mean / CV |
|---|---|---|---|---|---|
| Uniform/2 | run_6f55764291044af190a5b7c7f427b532 | 45daf4dc1df2eb987b801f47cf8d8445 | 977/23 | 8/0 | 1.004 / 0.004 |
| Skewed/2 | run_e216b7b6b2ad4da8a477c0f54e4db28f | 2819e04167ea9098573d6a8a9de29027 | 992/8 | 8/0 | 1.388 / 0.388 |
| Uniform/4 | run_3fd4881c0fa3487d8421f7c8f4e2bdc9 | 1675d73229f3a01a600b0fdee7a72f5d | 985/15 | 8/0 | 1.068 / 0.0529905652 |
| Skewed/4 | run_cbd8dc1f2d6b4ea59e9f03dfb48443ab | 3aa1f7c3bc2118b7a7671200f304b78b | 986/14 | 5/0 | 2.188 / 0.6860553913 |

4 Run 모두 VALID + UNKNOWN,1332 ACK/완료/유일 ID, 중복·누락0, 최종 Measurement 미처리0/read backlog0, Restart0, 실제 목표/Sent/ACK 코호트 Rate100.0 records/sec다. Measurement 생성1000건과 Drain 처리량을 분리했다. 유효한 Histogram·CPU 샘플·Subtask 처리율/Count·Busy/Idle/Backpressure를 저장했고 일부 신선도/샘플 범위 부족은 원시값·사유 그대로 남겼다.

Skewed의 생성/발행/ACK Hot 수는 두 P 모두394건(39.4%)이었다. 최종 Aggregation Measurement Hot sum도394건이고 전체 코호트 sum도1000건으로 일치했다.

| P | 실제 Hot Subtask | 관측 Key Group | Subtask별 Measurement 코호트 처리 수 | Hot 처리 수 |
|---|---:|---:|---|---|
| 2 | 1 | 86 | [306, 694] | [0, 394] |
| 4 | 2 | 86 | [157, 149, 547, 147] | [0, 0, 394, 0] |

Gauge는 실제 Hot 처리 뒤 값이 등록됐으며 원시 Counter/생성·ACK 수 대조와 신선도를 확인해 OBSERVED_SINGLE_SUBTASK로 판정했다. 예상 배치를 출력하지 않았다. Uniform에는 Measurement Hot 이벤트가 없어 NO_HOT_EVENTS이며 담당 Subtask를 지어내지 않았다.

이 결과는 테스트한 P2/4 각각에서 한 Hot Key의 측정 코호트가 한 Subtask에 남고, 균등 Kafka Partition 부하에서도 Aggregation의 처리 Count가 불균형해질 수 있음을 보여준다. 다만 낮은 입력률에서 Aggregation 포화 후보는 관측되지 않았다. 실제 Sink 코호트 처리율은97.7/99.2/98.5/98.6 records/sec였다. 짧은 경계 영향·Clock/Histogram·초기 메트릭·JIT·1회 반복 때문에 Skew의 Capacity 감소나 P 증가의 지속 성능 개선/실패를 증명하지 않는다. 결과를 가설에 맞춰 수정하지 않았다.

실험3의 실제 occupied Slot은 Baseline2, Uniform High8, Focused8이었다. Operator Subtask 합은 각각10/40/18이므로 Focused의 Subtask 수가 적다고 CPU 예산 또는 Slot 사용량이 줄었다고 주장하지 않는다. 메모리·Core-seconds/평균 사용 코어/관측 범위는 실제 분석 표로 제공하며 원인·자원 효율을 단정하지 않는다.

### 실측 분석 생성과 최종 정리

```bash
docker compose run --rm analyzer --experiment 3 --session results/experiment_3/session_1950c0073c0443d2ac71dbbc565a321f
docker compose run --rm analyzer --experiment 4 --session results/experiment_4/session_8a3381c7f9b04c4b97bb8b26bc0d9069
```

실제 성공 결과:

- 실험3: `analysis_1e859990d6474f66bd27565c0ff4ad5f/`, 실측 PNG15개, summary.csv4 Run, subtask_diagnostics.csv86행.
- 실험4: `analysis_fdc0f9324804431a941547ca13871a95/`, 실측 PNG14개, summary.csv4 Run, subtask_diagnostics.csv60행.
- 두 분석 모두8절 한국어 report.md, 개별/반복/Operator/Subtask/Latency 추세 CSV, comparisons/diagnostics/environments/analysis JSON 생성. PNG·CSV 행수/보고서8절·1회 Std 결측·CPU 비용 Raw 재계산 표시를 실제 확인했다.
- Heatmap은 해당 Operator의 실제 P를 라벨로 사용한다. Focused의 Heavy8을 Aggregation4 또는 Source2의 P라고 표시하지 않도록 보정했다. Busy/Idle/Backpressure 색 범위는0~1000ms/sec로 고정해 짧은 저부하 값이 상대색만으로 포화처럼 보이지 않게 했다. Missing/미설정 Subtask는 마스킹하며 실제0만0이다. 실제 생성 그림도 열어 확인했다.
- 정리: Phase4의8 Run 모두 Job CANCELED, Producer/Probe/Observer 제거 및 Topic 삭제 확인, cleanup_errors=[], Drain timeout=false. 최종 실제 REST 활성 Job0개, Kafka/JM/TM healthy. 서비스와 과거 Raw/Checkpoint는 유지했다.

마지막 분석 라벨 보정 뒤 컨테이너 Python75개 테스트도 실패/오류/Skip0을 확인했다. Load 실행 중에는 분석/이미지 빌드/다른 Benchmark를 동시에 실행하지 않았고, 실제 측정이 끝난 뒤 분석과 Unit만 병렬 실행했다.

### Phase 4 완료 판정과 미검증

기능 완료 기준을 충족했다: 실제 Operator/Subtask 병목 진단 계측, 기존 Uniform/Skewed Generator 검증과 관측 분포 대조, Kafka Partition 비교, Hot 배치·부하 Max/Mean/CV/샘플 CPU, 실험3/4 Smoke, 실제 Heatmap/표/한국어 분석 리포트 및 정리. 실제 Source/다른 Operator 선포화와 의도한 병목의 충족 여부를 자동 판정하고 불확실을 보존한다.

**미검증**: 장시간 Full의 Heavy 또는 Aggregation 포화 병목, Scale-out의 지속 성능/자원 효율 개선, Hot Key에 의한 Capacity ceiling, P8/16 Skew 실측, 통계적 유의성, CPU/계측 on-off 정량 비용, Production SLO 및 Capacity 기반 운영 추천. 사전 점검은 불확실이고 짧은 Smoke의 포화 후보도 없으므로 해당 가설을 검증 완료로 표시하지 않는다. CPU/자원 quota 경고는 보존했으며 인과로 해석하지 않았다. Salting/Two-stage 및 정확한 전체 Subtask CPU/State cardinality는 미구현이다.

다음 Phase5에는 Partition8 vs Source P2/4/8/16의 실제 Assignment/Idle Source 실험, 실험5/all CLI, 전체 최종 리포트·재현성 검토를 구현한다. 이번에는 실험5/all 실행기나 해당 비교 그래프를 구현하지 않았으며 Full 부하도 실행하지 않았다.
# Phase 5 검증 (2026-10-09, 최종 기록)

현재 요청은 Phase5이다. 이전 Phase0~4 코드·실측·테스트 기록을 재사용하고 기존 results를 보존한다. Repository는 최초 Commit이 없고 기존 파일이 untracked여서 Git SHA는 null이다. Docker 소켓은 Sandbox에서 `operation not permitted`였고 승인된 Docker/실행 명령으로 실제 환경을 검증했다. Kafka/JM/TM은 실제 healthy 상태로 재사용했다. Full Benchmark는 실행하지 않았다.

이번 구현: Source 실제 Assignment 콜백/Count0 계측, Source만 Scale-out하는 실험5, all 순차 Suite/frozen 계획/Artifact 검사/정리 실패 중단, 실험5 실측 분석/한국어 리포트, Mode/명시적 Suite 기반 통합 블로그, 재현성 문서·README 갱신.

## 실제 Build / Unit / 설정 확인

```bash
docker compose run --rm builder -B verify
python3 -m unittest discover -s tests -v
docker compose run --rm --entrypoint python analyzer -m unittest discover -s tests -v
python3 -m compileall -q scripts load-generator tests
docker compose config --quiet
python3 scripts/run_experiment.py --experiment all --mode smoke --config configs/experiments/suite-quick-smoke.json --dry-run
python3 scripts/run_experiment.py --experiment all --mode full --dry-run
```

Java17/Maven3.9.11 `BUILD SUCCESS`, Tests9/Failure0/Error0/Skip0. 새 테스트는 실제 Split 수락/중복 콜백/빈 Reader Count0·위임 계약이다. 기존 계산/State/Checkpoint harness·Phase4 CPU/Hot 계측 테스트를 재사용했다. Maven의 기존 deprecated API와 LICENSE/NOTICE/MANIFEST Shade 경고는 남지만 빌드 실패가 아니다.

Python Host3.14.4: Tests91, 통과87/Skip4(Host의 pandas/matplotlib 부재). 고정 분석 컨테이너 Python3.12.12: Tests91, 모두 통과/Skip0, 최종 확인10.124초. 신규16개 테스트는 Source 할당 Missing/재시작/신선도/중복/읽기 충돌·고정 Downstream·종료 정리·all Dispatch/중단·Mode/미완료 Session 필터·Not Executed/반복 통계·분석 출력이다. Fixture는 임시 디렉터리이며 실측 results와 섞지 않았다. compileall/Compose config도 종료0.

quick Smoke dry-run은14 Run, 설정 Load+최대Drain 합계672초이며 Setup/API/정리 시간은 별도다. 기본 Full dry-run은 최대270 Run(실험1/2/3/4/5 각각135/87/12/24/12), Load+Drain 설정 합계83,700초다. 이는 실제 Full 소요 시간이나 성능 측정 결과가 아니다.

## 실제 기존 E2E 회귀

`python3 scripts/smoke_test.py` 종료0. 실제 Job `a67279c5f83c6219c1110446766930fb`, ACK600/완료600/유일eventId600/중복0/파싱오류0/Checkpoint8. Graph의 독립 P와 새 Source Adapter를 포함한 실제 Pipeline이 동작했다. `VALID + UNKNOWN`, Capacity=null. Producer 삭제, Job CANCELED, Topic 삭제 확인, cleanup_errors=[].

## 실제 전체 축소 Smoke Suite

```bash
python3 scripts/run_experiment.py --experiment all --mode smoke --config configs/experiments/suite-quick-smoke.json
```

첫 Suite: `results/suites/suite_f11a6cacfed0484b97aecc6de1691ad1`. 축소 행렬은1(P1 탐색/고정부하)2 Run,2(P1 Calibration/독립 probe)2 Run,3 사전점검+3 Case4 Run,4(P2 Uniform/Skewed)2 Run,5 SourceP2/4/8/16의4 Run이다. 기본20 Run의 all Smoke와 구분한다.

### 발견 문제와 첫 Suite 중단

실험1/2 각2 Run은 VALID+UNKNOWN. 실험3의 low_baseline `run_990858bc4dd842edaac6bffcf3b10ca4`는 Preflight에서 발행500건/LEO500/실제100 records/s였지만 Topic 삭제 CLI가 `Timed out waiting for a node assignment. Call: listTopics`로 실패했다. 기존 코드가 Preflight.cleanup_errors를 상위 cleanup_errors에 전달하지 않아 INVALID+UNKNOWN과 빈 상위 cleanup_errors로 기록한 문제를 발견했다. 원시 실패를 변경하지 않는다. 당시 실험3 나머지3 Run은 VALID+UNKNOWN, 세션 COMPLETED_WITH_RUN_FAILURES였다.

계속 실행 중이던 Suite에 해당 프로세스만 SIGINT를 보내 안전 중단했다. 실험4 Uniform은 VALID+UNKNOWN, Skewed `run_dd64e99e132a476a9d143b7dc1ea3249`는 중단으로 ERROR+UNKNOWN, 세션 ERROR이며 실험5는 Not Executed. 첫 Suite는 종료코드1/ERROR,10 Run(VALID8/INVALID1/ERROR1), 마지막 Session 오류로 다음 실험을 거부했다. metadata/summary/lifecycle와 최초 판정은 모두 보존했다.

중단 후 해당 Preflight Topic `lab-preflight-c78d51cf5d8e46d6bfbdcb049c19dac3`만 삭제하고 실제 Topic 목록이 `__consumer_offsets`, `lab-events`뿐임을 확인했다. Kafka CLI Timeout의 원인은 확정하지 않았다. 측정 결과나 임계값을 유리하게 바꾸지 않았다.

수정: Preflight의 실행 예외는 이미 Rate가 성공했어도 valid=false/error로 남긴다. Interrupted 플래그를 상위 실행에 전달해 다음 Case가 시작되지 않게 한다. Preflight 실행/정리 오류는 Run ERROR+UNKNOWN이며 nested cleanup_errors를 상위 Summary/metadata에 전파해 기존 각 Phase 및 Suite의 중단 경로가 적용되게 했다. 정상적인 Producer Rate 미달은 기존처럼 INVALID다. 해당3개 신규 테스트를 포함한 분석 컨테이너 Unit94개/실패0/Skip0,10.436초를 확인했다.

추가 코드 검토에서 Host 절대 Session 경로가 분석 컨테이너 `/workspace`에서 읽히지 않는 문제를 수정했다. Suite에 상대 경로를 보관하고 명시적 Manifest의 실험/Session ID로 Suite가 위치한 결과 루트에 재결합한다. 이전 절대 경로만 있는 Manifest도 동일 ID로 읽으며 다른 실험/경로는 거부한다. Host→컨테이너/Archive 이동 Unit을 추가했다.

재검증은 새 UUID Suite에서 수행한다. quick Smoke의 순서는 Seed7 랜덤 `[5,1,4,2,3]`으로 사전 등록해 Phase5 새 기능을 먼저 확인한다. 행렬/부하/자원/판정 기준은 유지하며 첫 Suite의 frozen 설정도 그대로 보존한다. 측정 중 Code/JAR/Producer/Compose는 고정한다.

### 실제 실험5 관측 (재검증 Suite의 첫 Session)

Session: `experiment_5/session_02d4cdab1d144b0db48828fbfb4a6127`,4 Run. 실제 Graph는 Source P만2/4/8/16으로 바꾸고 Downstream P8/CPU·Keyed iterations0, Kafka8, 동일 고정 예산이다. 모든 Source 할당 판정은 OBSERVED_COMPLETE이며 소유자 변경·Restart·정리 오류는0이다.

| Source P | Run | 실행 유효성/안정성 | Measurement Sent | 시간 내 코호트 완료 | Drain 완료 | 실제 Reader별 Partition Count | 실제 미할당 Reader |
|---:|---|---|---:|---:|---:|---|---|
| 2 | run_cd7a0b5dc88b409c8e2267f5d25260dd | INVALID/UNKNOWN | 999 | 981 | 18 | [4,4] | 없음 |
| 4 | run_97e1a9962bc5434785470b388e901d77 | VALID/UNKNOWN | 1000 | 981 | 19 | [2,2,2,2] | 없음 |
| 8 | run_9fbaa1031fdd499897fa96d6137e550b | INVALID/UNKNOWN | 999 | 981 | 18 | [1,1,1,1,1,1,1,1] | 없음 |
| 16 | run_a113ea1e206d4860a33229782ea188fd | VALID/UNKNOWN | 1000 | 989 | 11 | [0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1] | 해당 Run의0~7 |

P2/P8은 Producer가 Measurement 마지막 예정1건을 expired 처리했다. Actual/ACK99.9 records/s로 ±2% 안이지만 기존 계약은 expired>0도 INVALID로 판정한다. 임계값을 변경하지 않았고 해당 Run을 유효 처리량 비교에서 제외한다. 실제 일정·발행·ACK/Queue/Retry/CPU/Lateness는 producer.json/jsonl에 보존하며 원인을 확정하지 않는다. Slot/Reader의 실제 Assignment 관측과 유효한 성능 비교는 구별한다.

네 Run 모두 Measurement 발행분 최종 미처리0, Source read backlog0, 실제 전체 유일 eventId1307/1308/1307/1308(준비8+Warm-up300 포함), 중복·누락0, Checkpoint 완료27/26/30/27/실패0, Restart0, Job/Producer/Topic 정리 확인. P16 미할당 Reader는 numRecordsOut=0, assigned Count=0, pendingRecords 미등록이고 할당 Reader8~15는 각 Partition1개와 실제 읽기/Count를 관측했다. 해당 결측 pendingRecords를0으로 채우지 않았다. Kafka 전체8 Partition에 유일한 소유자를 확인했다.

P4/P16 비교 전제 VALID, P2/P8은 입력 실행 전제로 INCONCLUSIVE. 어느 Run에도 Source 포화 후보가 없어 Source Capacity 또는 P8 이상 증설의 성능 효과를 주장하지 않는다. 실험5 Session은 COMPLETED_WITH_RUN_FAILURES다. Suite는 정리 성공한 INVALID를 보존하고 다른 실험을 계속 수행하며 최종 상태는 모든 Session을 반영한다.

### 전체 재검증 Suite의 최종 상태와 수집 오류 수정

`results/suites/suite_4b991cf19ca2404db35cd03616b51716`: 실험5/1/4/2/3 모두 실행,14 Run(VALID11/INVALID2/ERROR1), 모두 UNKNOWN, 종료1/COMPLETED_WITH_RUN_FAILURES. Suite 자체 errors=[]이고 미실행 실험은 없다. 실험2/4 각2 Run, 실험3 사전점검+3 Case4 Run은 VALID다. 정상 정리된 실패 Run을 성공으로 바꾸지 않았다.

실험1 Session `session_0bb1964defa04f6bbd04693667066dec`의 탐색 Run은 VALID, fixed_input `run_d859cb93e25a487784c445a172399ef0`는 Measurement 중 `JSONDecodeError('Expecting value: line 1 column 1 (char 0)')`로 ERROR였다. Job/Producer/Topic 정리는 성공했다. 당시 원시 stdout이 없어 정확한 응답과 원인은 확정할 수 없다.

수집 경로를 점검해 Kafka Probe의 stdout(JSON)과 stderr(진단)를 분리했다. 빈/잘못된 JSON은 설정된 최대2회 실제 요청으로 재시도하고, 원문·명령·시각·attempt·returncode를 `kafka_probe_diagnostics.jsonl`에 보관한다. nonzero/exhausted 응답은 ERROR로 남기고 값을 만들거나0으로 대체하지 않는다. Producer가 아직 실행 중이면 원래 수집 오류를 Producer wait 오류로 가리지 않는다. 최초 실패 데이터는 수정하지 않는다.

실행 유효성과 할당 증거는 독립적이다. OBSERVED_COMPLETE인 INVALID Run도 상태를 표시해 할당 Count Heatmap에 남긴다. 유효한 처리량 비교는 계속 VALID+비교 전제 VALID만 사용한다. 원시 추세/CSV에도 실행 상태를 표시한다.

최종 Unit: 분석 컨테이너 Python98개 통과/실패0/Skip0(6.273초). Host98개 중93개 통과/5개 Skip(분석 의존성 부재,1.079초). 추가 검증은 JSON transport stderr 분리·빈 응답 재시도 원문 보존·실패 유지, 실제 할당과 INVALID 처리량 분리, Suite 경로 이동이다. 임시 Fixture만 사용했다.

### 수정 후 독립 Smoke 검증

실험5를 새 Session `session_08186d814fb3492ab098a86cba361466`에서 `--target-rate 20`으로 재실행했다. P2/4/8/16 네 Run 모두 VALID+UNKNOWN, 실제 할당 OBSERVED_COMPLETE, Session COMPLETED/종료0이다. 이전100 records/sec 실행과 서로 다른 조건이며 통합하지 않는다. 판정 임계값·리소스 예산·Workload·Partition·Graph는 유지했다. 짧은 저부하 실행이므로 Capacity 검증은 아니다. 후속 표와 분석 링크에 코호트/Latency 표본·정리 결과를 기록한다.

| Source P | Run | 측정 생성/Sent/ACK | 측정 시간 내 코호트 완료 | Drain 완료 | 최종 read backlog / committed lag | 실제 미할당 Reader |
|---:|---|---:|---:|---:|---|---|
| 2 | run_28b2bb36df3542a3ba55dbe3d0c46b56 | 200/200/200 | 195 | 5 | 0 / 15 | 없음 |
| 4 | run_a0097df6e4fd4a5b89c1d212c6ad4bc3 | 200/200/200 | 196 | 4 | 0 / 9 | 없음 |
| 8 | run_2fd00bacdd744856842cbcfa61ad4177 | 200/200/200 | 196 | 4 | 0 / 14 | 없음 |
| 16 | run_053c99d2f1244c52be7cfd8d947123e3 | 200/200/200 | 195 | 5 | 0 / 13 | 0,1,2,3,4,13,14,15 |

각 Run은 준비8건·Warm-up60건·Measurement200건의 합계268건을 관측했다.

네 Run 모두 전체 유일eventId268/중복0/코호트 미처리0/Restart0, Job CANCELED 및 Producer/Topic 삭제 확인, cleanup_errors=[]. Partition Count는 P2=[4,4], P4=[2,2,2,2], P8=[1×8], P16=[0×5,1×8,0×3]. 실제 할당 위치는 이전100 records/sec Run의 인덱스와 다르며 예상 Hash 배치를 결과로 출력하지 않는다. P16 미할당 Reader의 pendingRecords는 null이다.

Checkpoint는 측정 구간 완료8/4/8/8·실패0/0/0/0, 전체 완료22/21/26/21·실패1/1/1/2였다. 초기 실패를 숨기지 않으며 Measurement 증가분과 전체 누적을 구별한다. 수집 Sample5/4/5/5, 구간 Coverage79.82%/59.62%/80.48%/80.57%, Latency 표본 각각20건으로 Full 안정 판정의 최소 기준을 충족하지 않는다. 각 Run의 resource_evidence_missing=true와 Kafka/Producer CPU Throttle 경고는 원본에 보존했다. 물리 자원의 충분함이나 p99 SLO 만족을 주장하지 않는다.

P4에서 Producer 종료 직후 실제 `docker exec`가 container not running/nonzero로 응답한 원문1건을 저장했다. 종료코드0 Producer의 마지막 실제 Kafka snapshot을 확인한 기존 race 처리 경로로 전환했으며 이후 별도 Admin 조회로 Drain을 확인했다. 빈/잘못된 JSON 재시도 발생은 이 네 Run에서 관측되지 않았고 해당 분기는 Unit으로만 검증했다.

실험1 수집 회귀 `--experiment 1 --mode smoke --parallelisms 1`: `session_4de58697a13f4ee1a1a27126ec7c31f7`, 종료0/COMPLETED, 두 Run VALID+UNKNOWN. 탐색 `run_20caad5189fa4c13a7318d51252f995d`/Job `2caf494a529aa00aabec5864efdbc169`는 측정 Sent1000/시간 내 코호트979/Drain21, fixed_input `run_9d1520fec8894617a4f9b6f9de123c9a`/Job `14c141a5bf93ccca50038b4fbf50ddf8`는1000/986/14. 각각 전체 유일eventId1332/중복0/최종read lag0/Restart0/cleanup_errors=[]. 측정 Checkpoint 실패0, 전체 실패1은 각각 남긴다. fixed_input의 Producer 종료 race(nonzero/No such container) 원문과 성공한 실제 마지막 snapshot을 보관했다. JSON 수집 ERROR는 재발하지 않았지만 최초 원인은 미확정이다.

### 실제 분석 출력

```bash
docker compose run --rm analyzer --experiment all --mode smoke --suite results/suites/suite_4b991cf19ca2404db35cd03616b51716
docker compose run --rm analyzer --experiment all --mode smoke
```

두 명령 모두 종료0, analysis_errors={}, 미실행 실험 없음, 한국어8절 블로그/CSV/JSON 생성. 첫 명령은 원래 Suite14 Run(11 VALID/2 INVALID/1 ERROR)을 정확히 선택해 [실패 포함 보고서](../results/reports/report_e6ec5bda2f5e45b7973165b0421ed744/blog-report.md)를 생성했다. 실험1 fixed_input ERROR의 처리량/Latency 등 비교 차트는 Not Executed이며 임의 데이터를 넣지 않았다. 실험5의13개 PNG 중 실제 Assignment Heatmap은 네 관측 Run, 공통 처리량 비교는 VALID인 P4/P16만 포함한다.

두 번째 [최신 선택 보고서](../results/reports/report_fc50fa8c30ce40e2872cffcd861ed194/blog-report.md)는 수정 후 실험1/5와 앞서 완료된 실험2/3/4를 명시적으로 선택했다.14 Run 모두 VALID+UNKNOWN이며 서로 다른 Artifact/Workload/Rate를 공통 Capacity로 합치지 않는다. 각 PNG는 실험1/2/3/4/5=6/6/15/14/13, 총54개다. 실험5 [개별 리포트](../results/experiment_5/session_08186d814fb3492ab098a86cba361466/analysis_e9f10981bee84ad095351628dbe176aa/report.md), Summary/반복/Assignment/Subtask/Trend CSV, diagnostics/environments/analysis JSON을 생성했다. 실제 Assignment·Subtask Throughput PNG를 열어 인덱스/결측/Count0 표시를 확인했다. 기존 결과와 리포트는 덮어쓰지 않았다.

최종 `python3 -m compileall -q scripts load-generator tests`, `docker compose config --quiet` 종료0. 측정 부하 종료 후 분석을 수행했다. 실제 Full Capacity·Predicted P·최소 안정/운영 P는 null/Not Executed다.

최종 리포트 설명 검토에서 Sink Rate가 “Measurement 시간 내 완료된 Measurement 생성 코호트 / Measurement 기간”임을 명확히 했다. 다른 코호트를 포함한 실제 시간 내 전체 완료는 observations.measurement_window_total_completed로 별도 보관한다. Source rate는 Counter 차분의 관측 부분 구간이므로 코호트/경계 차이를 숨기지 않는다. 수치와 원본은 변경하지 않았다. 같은 두 분석 명령을 다시 실행해 모두 종료0/errors={}였으며 [최종 최신 선택 블로그](../results/reports/report_a306f3d0e0c448168054d0a299989136/blog-report.md), [최종 원래 Suite 블로그](../results/reports/report_4688e9520cc2467298adf684b20f961e/blog-report.md)를 새 디렉터리에 생성했다. 실험5 최종 분석은 [report.md](../results/experiment_5/session_08186d814fb3492ab098a86cba361466/analysis_b97aeb4815ac47fd838be4812672caa7/report.md)이다. 앞선 분석도 보존한다.

### 최종 정리·완료 기준

실제 `docker compose ps --format json`: Kafka/JM/TM healthy, 실행 중 Producer 없음. Flink REST active_jobs=[], slots-total16/slots-available16/jobs-running0. Kafka 실제 Topic 목록은 `__consumer_offsets`, `lab-events`뿐이다. 과거 Job51개 CANCELED/1개 FAILED라는 누적 이력을 삭제하지 않았고 이번 실행의 Job은 각 Run 정리 기록으로 구분한다. 서비스와 Checkpoint 자료는 보존했다.

| Phase5 완료 조건 | 실제 근거 / 판정 |
|---|---|
| 실험5 Smoke 실행 | Rate20 SourceP2/4/8/16, 모두 VALID+UNKNOWN, 실제 할당 OBSERVED_COMPLETE / 충족 |
| 전체 실험 CLI | all 축소14 Run·frozen 계획/Hash/Manifest·실패/정리 정책·dry-run / 충족, Suite 전체 성공은 아님 |
| Raw/Summary/그래프/한국어 리포트 | 실제1~5 Session·CSV/JSON·최신 선택 PNG54개/8절 보고서 / 충족 |
| 최소 E2E/Unit | 실제600건 E2E, Java9개/Python98개(분석 컨테이너) 통과 / 충족 |
| 재현·기술 한계·미검증 공개 | README/reproducibility/limitations/본 이력·원본 실패 보존 / 충족 |
| 장시간 Capacity·예측 P 독립 검증 | Full 미실행 / Not Executed, 프로젝트의 과학적 최종 목표는 아직 미검증 |

빌드의 실제 JUnit XML/TXT, Python Unit/E2E/Smoke/Suite/분석 로그와 dry-run 계획을 `results/validation/phase5_20261009_final/`에 복사하고 SHA256 manifest를 남겼다. Fixture와 성능 원본을 혼합하지 않았다. 최초 Git Commit은 없어 Git diff는 새 파일을 표시하지 않으며 Git SHA=null이다. 기존 사용자 결과와 파일을 삭제하거나 커밋하지 않았다.

## 2026-10-28 블로그 첨부용 실험 증거 보고서

사용자 요청에 따라 기존 실측을 추출해 [실험 증거 보고서](experiment-report.md)를 작성했다. 런타임 구현과 기존 results/ 산출물은 변경하지 않았다. Git 최초 커밋이 없고 기존 파일 전체가 untracked인 상태를 확인한 뒤, 새 문서/첨부 파일을 추가하고 README에 링크를 추가했다. 작업 중 실제 `docker ps` 출력은 빈 목록으로 실행 중 컨테이너가 없었다. Docker 소켓 조회는 최초 sandbox 권한 거부 후 승인된 조회로 확인했다.

선택은 기존 `report_a306f3d0e0c448168054d0a299989136`의 주요14 Run과 과거 실험4 `session_8a3381c7f9b04c4b97bb8b26bc0d9069`의 보조4 Run으로 고정했다. 둘을 반복 통계로 합치지 않았다. 원래 Suite의11 VALID/2 INVALID/1 ERROR와 수정 후 독립 재검증을 구분했다. 전체 experiment_*/session_*/runs/*/summary.json46개를 확인했고 VALID41/INVALID3/ERROR2, 모두 smoke+UNKNOWN이었다. 별도 phase2/smoke/preflight와 Fixture는 이 집계에서 제외했다.

실행한 해당 범위 검증은 다음과 같다.

- 표준 라이브러리 기반 일회성 추출/대조 스크립트 실행 성공. 주요14/보조4 구분,18개 실제 Run/Job ID·설정·Artifact Hash·Producer 생성/Sent/ACK·시간 내 완료/Drain/최종 완료·Restart0·최종read backlog0·정리 오류 없음 확인.
- Aggregation의 최종 측정 코호트·Hot 처리 Counter를 해당 Run의 마지막 raw_samples.jsonl REST frame과 대조했다. 주요/보조 Uniform/Skewed의 실제 Counter·Kafka TV0·Max/Mean을 재계산했고 보조 P2/4의 Hot394건이 각각 단일Subtask1/2에 남는 것을 확인했다.
- 실험5 네 Run의 실제 Count를 마지막 raw frame과 대조했다. 모든 Reader Count/ID,Partition0~7의 중복 없는 유일 소유자,실제 미할당 인덱스를 확인했다. 추출 과정의 진단 필드 경로를 `source_assignment`로 맞춘 뒤 네 Run의 할당 필드가 모두 존재하고 값이 일치함을 확인했다.
- [manifest.json](experiment-evidence/manifest.json)의 원본160개 SHA256과 CSV/JSON/색인3개 첨부 Hash를 재검증했다. PNG2개는 기존 실측 그림과 byte 동일하고 시각적으로 확인했다. Fixture/합성 성능값은 사용하지 않았다.
- 보고서/색인의 로컬 링크48개와 README 진입 링크를 확인했다. 보고서 Run ID 끝8자리,실험3 의도 병목 미관측,Source 포화 미관측 및 UNKNOWN/null 설명을 원본과 확인했다. 두 CLI의 `--help`로 보고서 재현 명령의 `--session`/`--suite`/`--dry-run` 옵션을 확인했다.

추출 CSV/JSON은 [experiment-evidence/](experiment-evidence/run-index.md)에 보존하며 원시 파일 자체를 대체하지 않는다. 보고서 부록의 SHA256 재검증 명령을 실제 실행해 통과했다. 이번 변경은 문서와 증거 추출에 한정하므로 Java Build/Unit/Python 전체 Unit/Compose Build/분석기 재실행/Smoke/Full 부하 재실행은 **Not Executed (해당 런타임 코드 변경 없음)**이다. 과거 빌드·테스트 성공 기록을 이번 실행으로 재표기하지 않았다. 새 Phase 완료 판정이나 Capacity/운영P 검증 완료를 추가하지 않았다.

## 2026-10-28 Web UI 캡처용 실제 실행

사용자가 Running Jobs가0개인 Web UI에 캡처 가능한 Job을 요청했다. 기존 JobOptions/BenchmarkJob/submit_job.py/lab.py/producer.py/Compose와 Git 상태를 확인했다. 실행 전 JobManager/TaskManager는 healthy였고 Kafka는 중지되어 있었다. REST에서 active Job이 없는 것을 확인한 뒤 Kafka를 시작하고 기존 Flink 서비스·JAR·Producer 이미지를 재사용했다. Docker 소켓은 sandbox 권한 거부 후 승인된 실행으로 접근했다. 기존 성능 Run이나 결과를 변경하지 않았다.

- 목적: `UI_SCREENSHOT_DEMO_NOT_BENCHMARK`. 전용 Topic/Group/Checkpoint 경로,Run ID `ui_20261028T143228Z_5f7463a8`,Job ID `d0e248817edf46f641dce2a5b8789da4`.
- Kafka8 Partition,SourceP16/ParseP4/HeavyP4/AggregationP4/SinkP4,Heavy0/Keyed200000,목표100records/s,Skewed Hot설정40%,round-robin/Seed42/HashMap/maxP128/Chainingoff/Checkpoint5000ms.
- 실제 JAR SHA256 `8123d3f44f1dad91ceb42a8c906ac3f2a72abc155d112fc4078afe12d7823511`. Java/JAR/Producer/Compose 구현은 수정하거나 빌드하지 않았다. 캡처를 위해 기존 Producer를 detached로7200초 실행했다. 시작2026-10-28 23:34:12 KST,입력 종료 예정2026-10-29 01:34 KST. Flink Job은 Unbounded로 Cancel까지 실행되며 Producer 종료 후에는 Idle이 될 수 있다.
- 실제 2026-10-28 23:36 KST 검증: Job 및5개 Vertex 모두RUNNING,RunningJobs1,TM1/Slots16/Available0. Producer Running=true. 기존 submit/wait_running 및 snapshot_subtasks를 재사용했다.
- 실제 Source Count=[1,1,1,1,1,1,0,0,0,0,0,0,0,0,1,1]. 모든 Reader Count/ID와 Partition0~7의 중복 없는 전체 소유자를 확인했다. 미할당은Subtask6~13이며 읽기 기록0으로 추정하지 않았다.
- Aggregation의 누적 코호트 처리 Counter=[1821,1804,6579,1813],Hot Counter=[0,0,4782,0],실제HotSubtask2/KeyGroup86. 순차REST 관측이며 각 Counter의 동시합이나 기존 실험394건과 혼합하지 않는다. Sink 완료12135건으로 최초 검증437건보다 증가했다. CheckpointCompleted25/Failed2로 초기 실패도 보존했다.
- 실제 Docker Inspect에서 TM quota2CPU/2560MiB,Producer0.5CPU를 확인하고 실제 이미지ID/다른 서비스 예산과 함께 저장했다. UI 루트HTTP200/제목Apache Flink Web Dashboard를 확인했다. 사용자 브라우저의 실제 화면 캡처는 수행하지 않았다.

산출물은 [캡처용 manifest](../results/ui_demo/ui_20261028T143228Z_5f7463a8/manifest.json),capture-verification.json/capture-subtasks.json/capture-environment.json/capture-job.json/capture-ui-check.json,초기 검증Snapshot이다. Producer의 최종ACK Summary는 종료 후 기록되며 검증 시점에는 미완료다. 별도 Controller Preflight/측정·Drain/성능 판정이 없는 대화형 데모이므로 Execution Validity=INCONCLUSIVE,Performance Stability=UNKNOWN으로 기록했다. Capacity·SLO·병목 포화·Full/최소운영P는 검증하지 않았다.

[캡처 안내](ui-capture-guide.md)에 실제 Job 링크,Graph/Source/Hot Key/Checkpoint/Slots의 권장 화면·정확한 메트릭 이름·캡션,동일 구성 재현과 현재 실행만 중지하는 명령을 추가하고 README에 연결했다. 임시 실행 도우미의 Python문법,문서의 로컬 링크·실제ID/Counter·Checkpoint 일치,재현/중지 명령의 Bash문법을 확인했다. `docker stop --help`로--signal/--timeout 옵션을 확인했다. 런타임 코드 변경이 없어 JavaBuild/전체Unit/이미지Build는Not Executed이며 FullBenchmark는 요청 범위가 아니므로 실행하지 않았다. 사용자의 캡처를 위해 Job·Producer·Kafka/Flink를 실행 상태로 남겼고 중지 명령은 실행하지 않았다.

