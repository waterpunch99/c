# 단계별 구현 계획

각 Phase는 Repository 확인 → 요구/완료 조건 확인 → 코드 작성 → Build/Unit/Integration → 오류 수정 → 실제 검증 기록 → 완료 판정 → 다음 작업 순서로 진행한다. 현재 구현 범위는 Phase 5이며 Phase 0~4를 재사용한다. Full 실제 실행과 새로운 후속 Phase는 요청하지 않은 범위이다.

| Phase | 구현과 산출물 | 완료 근거 |
|---|---|---|
| 0 | Repository/환경/버전 조사, benchmark-spec, architecture, AGENTS, 자원·계측 계획 | 실제 환경 기록, 공식 버전 근거, 실행 자원 계획, 명세·단계 계획 |
| 1 | Compose KRaft/JM/TM/Topic, Java 전체 Pipeline, 기본 Python Producer, P/CPU/Backend/Checkpoint/Chaining 설정, Unit/E2E | Java17 Maven verify, compose config, 서비스 요청 준비, 실제 Job RUNNING, Kafka ACK, Sink exact bounded ID audit, 핵심 Unit 통과 |
| 2 | Open-loop 생산 계측 확장/Preflight, REST/Kafka Offset 수집, 구간 자동화, 격리, 상태/지속 판정, CSV/JSON | 목표/Actual/ACK 비교, 실제 Job ID/Subtask 메트릭, Missing/상태/오염 방지 테스트, 자동 Run 1개 |
| 3 | 실험 1 Capacity 단계/범위 탐색·고정 Rate, 실험 2 Capacity 근거·공식·독립 검증, 표/그림 | 1/2 Smoke, 공식 Unit, 예측/관측 분리, Full 재현법 |
| 4 | 실험 3 Operator P·병목, 4 Uniform/Skewed·Partition 정합·Hot Subtask, Heatmap | 실제 병목 계측, 분포/불균형 테스트, 3/4 Smoke |
| 5 | 실험 5 Source Partition Assignment, 전체 분석 CLI/PNG/한국어 리포트, 재현성 검토 | 5 및 전체 CLI Smoke, 실제 데이터 기반 Raw/Summary/리포트, 미검증 항목 공개 |

## Phase 0 구현 순서

1. 기존 파일·Git·상위 AGENTS, Docker/Java/Maven/Python/CPU 확인.
2. 공식 Flink 릴리스·Connector·Docker 이미지와 의존성 확인.
3. 전체 계약을 benchmark-spec에 저장하고 현재 범위와 구분.
4. fixed-resource, Slot Sharing, State 격리, 메트릭의 의미/한계를 architecture에 결정.
5. 실제 검증은 validation에 기록. 버전 문서 확인과 실제 호환 실행을 구분.

## Phase 1 구현 순서

1. 고정 버전 Compose, 내부/외부 Kafka Listener, metadata readiness, JM REST readiness, TM RPC liveness.
2. Java17 Maven 프로젝트/서비스 병합 Shade JAR, Flink core provided.
3. Schema/오류 계수/결정적 CPU/Keyed ValueState/Sink API v2 구현.
4. 별도 P/UID/maxP/Checkpoint/Backend/Chaining 옵션 유효성 검사.
5. Python Kafka Producer, ACK/Error/Partition 관측, 최소 Monotonic Rate, 신규 파일 정책.
6. 실제 Job ID 제출 CLI, REST 계측 기반 E2E, 전체 Smoke eventId Bitset 검증.
7. 실제 Checkpoint·실제 Graph P·안전 종료 검증. 실패 결과 저장.
8. 한국어 README와 정확한 명령·Troubleshooting·한계 작성.

## Phase 2 구현 상세

- 별도 session Producer: Scheduled/Generated/Sent/ACK 시간 버킷, Retry/Queue/Send Latency, 목표 ±2% 조건, 별도 Topic Kafka Preflight. 단일 worker만 지원하고 round-robin/random 전략을 userId와 분리한다.
- kafka Admin API로 Partition LEO/Commit, Source read offsets/pendingRecords를 각각 수집하고 의미/지원 여부 확인. 실제 Source Assignment 관측 어댑터.
- 환경 수집 완성: cgroup quota/Throttle, JVM·GC, Slot 배치·사용, Host 변화, Clock 오차 판정, State 크기 지원 여부.
- Warm-up/Measurement/Drain 생성 코호트·관측 wall clock 분리. 측정 초기 Backlog 정책과 Latency 검열 처리.
- 설정 파일 기반 VALID/INVALID/ERROR/INCONCLUSIVE 및 STABLE/UNSTABLE/UNKNOWN 판정. Backlog 회귀·불확실성·변화량과 Latency/Checkpoint/Restart를 함께 판단.
- append-only Raw samples.csv/subtasks.csv, Summary, Run Manifest, SIGINT/오류 경로 정리 테스트. 실제 설정 불일치·생산 미달·수집 실패도 보존.
- `scripts/run_once.py`는 단일 Run만 자동화한다. 진단 Chaining off, cgroup v2 리소스 계측, 새 Source next-offset Gauge와 실제 등록 Partition 증거를 사용한다. Prometheus, 정확한 Subtask CPU 귀속, State cardinality는 구현하지 않았다.
- Smoke는 구간/수집/판정/정리를 검증하며 STABLE Capacity를 확정하지 않는다. 실제 완료 근거와 실패 이력은 `docs/validation.md`에 기록한다.
- 완료 판정: Phase 2의 목표/실제 Rate, Job/Subtask 수집, Missing/독립 상태/격리/저장 테스트, 실제 단일 자동 Run 성공 조건을 충족했다. Python 34개/Java 5개 Unit과 실제 Uniform/Skewed/최종 이미지 Smoke를 확인했다. Full 안정성은 실제 미실행이며 후속 탐색에서 추가 검증한다.

## Phase 3 구현 계획 (이전 구현)

측정 품질 검증을 재사용해 P별 Rate 탐색·고정 부하 비교를 만들고, 병목 근거가 있는 경우에만 Operator/Subtask Capacity를 추정한다. 병렬도 공식 계산과 독립 Run 검증, 반복 Summary/그래프를 구현한다. Phase 2의 단일 Run 처리량을 최대 Capacity라고 사용하지 않는다.

1. Phase 2 단일 Run을 실제 Experiment ID/조건/역할과 함께 호출한다. 기존 기본 CLI는 유지한다.
2. 실험 1: P별 적응형 Rate 증가/범위 축소, 제한된 시도·반복·랜덤 순서, 고정 입력률 비교. Stable 하한/Unstable 상한/미판정·우측 검열을 보존한다.
3. 실험 2: 별도 Calibration Run과 Validation Run, 직접 Operator/Subtask 처리율·포화/균등성/자원 근거, 복수 P 추정치 비교. 조건부 Capacity만 공식에 사용하며 근거가 부족하면 예측을 보류한다.
4. Smoke는 STABLE이 아니므로 기본 Smoke의 검증 단계는 계산값 검증이라고 부르지 않고 독립 Peak 부하 probe로 기록한다. 공식/주변 후보/Full 분기는 Unit Fixture로 검증한다.
5. Pandas/Matplotlib 고정 버전 컨테이너로 개별 Run·반복 통계·실측 PNG·한국어 리포트를 생성한다. Capacity/계산 오류가 결측이면 해당 차트는 Not Executed로 남긴다.
6. Java/Python Unit·Compose Build와 실험 1/2 실제 Smoke를 실행한다. Full 장시간 Benchmark는 실행하지 않는다. 검증 기록과 재현법을 갱신한다.

### Phase 3 완료 판정

2026-10-09 실제 검증: Java5개 통과, 분석 컨테이너 Python57개 통과/Skip0, 실험1/2 각각4개 Smoke Run 모두 VALID + UNKNOWN, 실험별 실측 PNG6개·CSV/JSON·한국어 리포트 생성, Run별 Job/Topic/Producer 정리 성공. 명세의 Phase 3 기능 완료 조건을 충족했다. 상세 명령·Session/Run ID·측정 코호트/Drain 건수·문제 수정은 `docs/validation.md`에 보존한다.

Smoke에서 Capacity를 생성하지 않았으며 실험2는 예측 보류 후 독립 Peak probe를 실행했다. 실제 Full Capacity 탐색/계산된 P의 장시간 검증은 Not Executed다. 해당 코드 경로는 분리된 Unit Fixture로만 확인했다. Full 실행 절차와 사전 비용 dry-run을 제공하고 요청 없이 실행하지 않았다. 다음 구현 범위는 Phase 4이며 기존 계획의 Operator별 병목 및 Data Skew 비교를 따른다.

## 후속 Phase 검증 정책

Capacity 탐색 실패·UNSTABLE도 유효 데이터로 보존한다. 예측 Run과 검증 Run ID를 분리한다. Subtask Capacity 근거가 부족하면 Operator 용량 추정을 생성하지 않는다. Full은 60/180초 × 최소 3회이며 실행 비용·조합 수를 CLI dry-run 계획으로 먼저 확인할 수 있도록 한다. 임의 그래프/성능 예측치를 실제 결과처럼 생성하지 않는다.

## Phase 4 구현 계획 (이전 구현)

1. 기존 Phase 3 실행·분석과 검증 기록을 확인하고 Phase 2의 Run 격리/Preflight/구간/판정/정리를 재사용한다.
2. 실험3의 고정 Operator Case A/B/C와 Focused 구성의 저작업량 사전 점검을 구현한다. 사전 점검과 실제 비교 Workload를 분리한다. 병목 후보는 실제 Subtask 메트릭으로 판정하며 자동으로 성공했다고 가정하지 않는다.
3. Aggregation 생성 코호트/Hot Key/측정 시간 내 Counter 및 실제 Hot 처리 이후의 Subtask/Key Group Gauge를 추가한다. Heavy/Keyed 연산 구간의 선택적 ThreadMXBean CPU 샘플을 구현하고 전체 Subtask CPU와 구분한다.
4. 실험4의 P별 Uniform/Skewed 쌍, 같은 반복 Seed/고정 물리 예산/Workload, 생성·발행·ACK Hot 비율 및 Kafka Partition 정합성을 검증한다. 관측 Counter를 병합해 Hot Key의 실제 배치를 확인하고 불충분하면 보류한다.
5. Subtask 입력/출력 Count·Rate·Busy/Idle/Backpressure/CPU 샘플, 처리 코호트 Max/Mean/CV, cgroup CPU core-seconds/메모리/실제 Slot을 저장한다. 처리율을 실제 유입 Key 분포라고 취급하지 않는다.
6. 실측 비교표·반복 통계·Subtask Heatmap·Partition/Hot 부하 그래프·한국어 리포트를 생성한다. 비교 전제 실패/결측/단일 분포 실행은 해당 비교를 Not Executed/불확실로 표시한다.
7. Java/Python 회귀·추가 Unit, 기존 E2E 및 실험3/4 실제 Smoke를 실행한다. Full/실험5/all 자동 실행은 하지 않는다. 실제 결과·문제·완료 기준·미검증을 validation.md에 기록한다.

### Phase 4 완료 판정

2026-10-09: Java8개, 분석 컨테이너 Python75개 통과/Skip0; 기존 E2E600건 회귀; 실험3/4 각각4 Run 모두 VALID + UNKNOWN 및 Run별 정리 성공. 실제 메트릭 기반 실험3 PNG15개/실험4 PNG14개와 개별·반복·Subtask CSV/JSON/한국어8절 리포트를 생성했다.

P2/4 Skewed의 생성·발행·ACK Hot394건(39.4%) 모두 각각 실제 Aggregation Subtask1/2에서 처리됐으며 Kafka Partition 쌍 TV0으로 비교 전제를 충족했다. 처리 코호트 불균형을 관측했고 포화 병목 진단 계측도 가능하다. 의도한 Heavy/Keyed 포화 후보는 저부하 Smoke에서 관측되지 않았고 사전 점검도 불확실로 보존했다. 따라서 기능 완료 조건은 충족하되 장시간 병목/Capacity·증설 효과는 검증 완료로 표시하지 않는다. Full/P8·16 및 계측 오버헤드는 Not Executed다.

상세 근거/Run ID/문제 수정은 validation.md, 재현·해석 기준은 phase4-design.md를 따른다. Phase5의 Source Partition 제약·실험5/all·최종 재현성 검토는 후속 구현으로 남긴다.

## Phase 5 구현 계획 (이번 작업)

1. Phase0~4의 명세·코드·실측/테스트 이력과 현재 Docker/Build 환경을 확인한다. 기존 Raw와 사용자 작업을 보존한다.
2. Kafka8 Partition, SourceP2/4/8/16, DownstreamP8 고정 조건을 구성한다. SourceReader 실제 할당 콜백을 계측하고 모든 Reader의 Count0/ID·신선도·Partition 전체 소유자·읽기 증거를 검증한다.
3. Source P만 변경하는 실험5 실행기를 기존 run_once로 구현한다. Downstream 선포화/입력 전제·실제 Source 후보를 별도 판정하며 미관측 Capacity를 생성하지 않는다.
4. 실험5 Raw/Summary/반복·Assignment/Subtask/Trend CSV, JSON, 실제 PNG, 한국어8절 리포트를 추가한다. 결측·실패·불확실성과 비교 조건을 보존한다.
5. 전체1~5의 frozen 계획/Artifact Hash/Session Manifest/안전 정리 기반 순차 Suite 및 드라이런을 제공한다. 실제 검증용 축소 Smoke 설정과 기본/Full 행렬을 구별한다.
6. 모든 실험 분석 CLI와 명시적 Suite Session 선택, 통합 한국어 블로그/CSV/JSON을 제공한다. Capacity 계산·독립 검증이 미실행이면 null/Not Executed를 유지한다.
7. Java/Python 회귀, Compose config, 기존600건 E2E, 전체 축소 Smoke Suite를 실제 실행한다. 발견 오류를 수정하고 관련 검증을 재실행한다. Full은 실행하지 않는다.
8. README/Troubleshooting/설계·계측/기술 한계/재현 문서와 최종 체크리스트를 실측 증거에 따라 갱신한다. 실제 완료 판정은 validation.md에 기록한다.

### Phase 5 완료 판정

2026-10-09: Java9개 통과, 고정 분석 컨테이너 Python98개 통과/Skip0, Compose config/compileall 성공. 실제 기존600건 E2E, 전체 축소 Suite14 Run, 수정 후 실험5 Source P2/4/8/16 네 Run 및 실험1 P1 두 Run을 실행했다. 실험5의 실제 할당·미할당 Count0·전체 Partition 소유자·읽기 증거를 확인했다. 전체 CLI의 Suite Manifest/안전 정리·실패 보존, Raw/CSV/JSON, 실측 PNG, 각 실험 및 통합 한국어8절 리포트, 재현/한계 문서를 제공한다.

기능 완료 조건을 충족했다. 원래 전체 재검증 Suite는11 VALID/2 INVALID/1 ERROR로 종료1이며 전체 성공으로 보고하지 않는다. Preflight 정리 오류 전파와 Kafka Probe JSON transport 문제를 수정하고 관련 Unit 및 독립 Smoke를 재검증했다. 최신 실험별 선택14 Run은 VALID+UNKNOWN이고 실측 PNG54개를 생성했지만 동일한 새 Suite를 모두 성공시킨 결과와 구분한다. 원시 실패·기존 리포트를 보존한다. 상세 Run ID·실제 명령·수치·수정 및 출력 링크는 validation.md를 따른다.

Full Capacity·예측 P의 장시간 검증·최소 안정/운영 P·Source 포화 성능·정량 계측 오버헤드·Event-time/복구·동적 Source는 이번 검증 범위 밖이다. 필요한 실행은 후속 사용자가 Full 비용 계획과 대표 Workload/자원·Producer 능력을 확인한 뒤 명시적으로 선택해야 한다. 새로운 후속 Phase를 구현하거나 요청 없이 Full을 실행하지 않았다.
