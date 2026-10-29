# Phase 5: Source 할당·전체 실행·최종 리포트

실제 검증에서 Producer expired가 발생한100 records/sec Run은 INVALID로 보존했다. 이후 `--target-rate 20`의 별도 조건에서 네 Source P를 다시 실행했으며 두 부하 조건은 합치지 않는다. Assignment는 독립 증거이므로 OBSERVED_COMPLETE인 INVALID Run의 Count도 상태와 함께 Heatmap에 남긴다. 공통 성능 비교는 VALID+비교 전제 VALID만 포함한다.

Kafka Probe JSON transport는 stdout/stderr를 분리하고 빈/잘못된 JSON만 설정된 최대2회 실제 조회로 재시도한다. nonzero/exhausted는 ERROR이며 원문은 kafka_probe_diagnostics.jsonl에 남긴다. 이전 원문이 없는 JSONDecodeError의 원인을 추정으로 확정하지 않는다. 재시도는 결측을0으로 대체하는 방법이 아니다.

## 실험 5의 통제 조건

[phase5.json](../configs/experiments/phase5.json)은 Kafka8 Partition, Source P=2/4/8/16, Parse/Heavy/Aggregation/Sink P=8 고정, Uniform/명시적 round-robin, Heavy/Keyed CPU0, Chaining off, maxP128, 같은 Docker CPU/Memory 예산을 사용한다. Smoke는 Rate100, Warm-up3/Measurement10/Preflight5초, 반복1, 수집2초다. Full은 기본 Rate1000, 60/180초, 반복3, 순서 랜덤이며 이번에 실행하지 않는다. Source/Downstream 실제 Graph는 run_once의 REST 검증을 재사용한다.

Downstream P8/CPU0은 Source 제약을 진단하기 위한 기본값이다. 그 선택 자체로 충분한 용량을 증명하지 않는다. 생성 분포/Partition, 실제 할당, Run VALID, 각 Downstream 최대 Busy<750ms/s, Downstream 병목 후보 없음 조건을 별도 `source_comparison_premises`에 기록한다. 기준은 설정 파일에서 고정한다. Smoke의 낮은 Busy는 다른 입력률/장시간 여력을 보장하지 않는다. Source 포화 후보가 없으면 고정 Rate에서 관측한 Throughput을 Source Capacity로 설명하지 않는다.

## 실제 Assignment 계측

기존 `TrackedKafkaDeserializer`의 Partition offset+1은 실제 deserialize의 증거다. 읽지 않은 Source의 Assignment 여부까지 증명할 수 없다. `ObservedKafkaSource`는 기존 KafkaSource의 Enumerator/Serializer/Checkpoint/Poll/Availability/Event/Pause/Close 호출을 위임하고 `SourceReader.addSplits`가 정상 수락한 Partition만 Gauge로 등록한다.

- `sourceAssignedPartitionCount`: Reader마다 등록되며 할당이 없는 Reader의 관측값은 실제0이다.
- `sourceAssignedPartitionN`: 해당 Partition을 수락한 Reader에만 등록한다. 예상 hash/round-robin 소유자를 출력하지 않는다.
- `sourceAssignmentSnapshotTimestampMs`: 각 Reader의 별도 Gauge 객체, REST frame 신선도 근사 시각.

단일 고정 Topic, 무한 Source, Discovery off(`partition.discovery.interval.ms=-1`), 신규 State, Restart0에서만 누적 콜백을 현재 할당으로 해석한다. 모든 Reader의 Count와 Partition 목록이 일치하고 신선하며, Kafka 전체 Partition에 정확히 한 소유자가 관측되고 읽기 Offset 증거와 충돌이 없을 때 `OBSERVED_COMPLETE`다. 누락/신선도/재시작 불확실성은 INCONCLUSIVE, 중복 소유/읽기 충돌/실행 중 소유자 변경은 INVALID다. 이것은 기존 Run의 Execution Validity와 독립된 계측 전제 판정이다. Gauge 미등록을 Count0으로 대체하지 않는다.

준비 단계는 모든 Partition에 실제 이벤트를 보내 Source→Sink 진행을 확인한다. Partition Gauge와 Count=0은 해당 관측 frame의 근거이며, REST는 분산 원자적 snapshot이 아니다. Topic 확장·복구·bounded Split 종료·동적 Kafka Source를 지원한다고 주장하지 않는다. Source/Subtask numRecordsOut 차분은 측정 구간에 들어가는 관측 부분 구간이며 전체 Measurement Sink Counter와 시간 범위가 다를 수 있다.

Kafka next read Offset, Admin LEO, Group Committed Offset, pendingRecords는 분리한다. pendingRecords는 Connector가 지연 등록하므로 미할당 Reader에서는 Missing/Unsupported일 수 있다. Job 합계는 모든 Reader가 관측되어야 계산하며 미할당의 결측을0으로 채우지 않는다. 각 Reader 원시 값·관측 Reader 수를 별도 남긴다.

공식 근거: [Flink2.2 SourceReader.addSplits 계약](https://raw.githubusercontent.com/apache/flink/release-2.2/flink-core/src/main/java/org/apache/flink/api/connector/source/SourceReader.java), [Kafka Connector5.0.0 metrics 구현](https://raw.githubusercontent.com/apache/flink-connector-kafka/v5.0.0/flink-connector-kafka/src/main/java/org/apache/flink/connector/kafka/source/metrics/KafkaSourceReaderMetrics.java), [Flink2.2 KafkaSource·Offset·Idleness](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/). 문서/소스 확인은 로컬 실행 검증을 대체하지 않는다.

기본 noWatermarks에서는 Event-time 지연을 측정하지 않는다. Event-time 도입 시 Partition이 없는 Reader 및 조용한 Partition의 Watermark 지연과 `WatermarkStrategy.withIdleness`를 별도로 테스트해야 한다.

## 전체 실행과 보존

`benchmark_suite.py`는 1~5의 기존 실행기를 순차 호출한다. 실행 전 Suite 전체 조건/비용을 고정하고 `suite_UUID/suite.json`에 Artifact Hash와 함께 보존한다. Session 완료마다 sessions.jsonl에 추가한다. Cleanup 실패/중단/Session 실행 오류 또는 실행 중 코드·JAR·Producer·Compose 변경은 다음 실험을 중단한다. Preflight의 nested 정리 오류도 상위 cleanup_errors로 전달한다. 정상적인 생산 속도 미달은 INVALID, 실행/정리 예외는 ERROR로 구별한다. INVALID/저성능 결과는 삭제하지 않으며 정상적으로 정리된 Run 실패는 Suite 종료 상태에 보존한다.

기본 all Smoke 최대20 Run. `suite-quick-smoke.json`은 실제 계통 재검증을 위한 명시적 축소안(14 Run): 실험1 P1의 탐색/고정2, 실험2 P1 Calibration/독립 probe2, 실험3 사전점검+3 Case4, 실험4 P2 두 분포2, 실험5 Source P2/4/8/16의4다. 실험 간 순서는 Seed7 랜덤 `[5,1,4,2,3]`이며 각 실험 내부 정책은 해당 Phase를 따른다. 같은 계획이라도 Setup/API/정리 시간이 추가되므로 dry-run의 시간 합계는 전체 wall-clock 상한이 아니다. 모든 결과의 Smoke UNKNOWN은 Capacity로 승격하지 않는다.

all의 --config는 `{"experiments":{"5":{"config":"configs/experiments/phase5.json","overrides":{"target_rate":100}}}}` 형식이다. 일반 개별 실험의 Phase 설정 파일과 구분한다. Full을 실제 실행할 때만 명시적으로 `--mode full`을 사용한다. 이번 작업의 Full은 계획/Unit 검증만 수행한다.

## 분석과 블로그

실험5는 개별/반복 Summary, Assignment 시계열 CSV, Subtask CSV, read/commit/pending/Latency 시계열, 실제 환경/진단 JSON, 처리율·Latency·CPU·Busy/Idle·Subtask Heatmap·Assignment Count·Lag PNG와 한국어8절 report.md를 생성한다. 미관측 메트릭/비교 전제 실패는 차트를 Not Executed로 남긴다. 고정 Downstream Graph를 비교 fingerprint에 포함하고 다른 물리 예산/Artifact/Workload/Mode를 한 곡선으로 합치지 않는다.

`analyze_results.py --experiment all --mode smoke --suite ...`는 정확히 해당 Suite의 Session을 분석한다. 상대 경로/실험/Session ID로 Suite가 있는 결과 루트에 재결합해 Host/컨테이너 절대 경로 차이를 처리한다. --suite를 생략하면 요청 Mode의 가장 최근 완료 Session을 실험별로 선택하고 manifest에 기록한다. 실험 간 처리량·Artifact가 달라 공통 Capacity/통계를 만들지 않는다. 누락 실험은 Not Executed다. 각 분석은 새 analysis_UUID, 통합은 새 reports/report_UUID에 Summary CSV/report.json/blog-report.md를 생성한다. Raw/기존 분석은 덮어쓰지 않는다. 분석 오류는 해당 상태를 보존하고 CLI는 종료코드1을 반환한다.

최종 리포트 중심은 “측정한 Capacity로 계산한 P가 적절했는가?”다. 실제 Full의 조건부 예측·독립 검증 근거가 없으면 null/Not Executed이며 설명용 Capacity로 채우지 않는다. 고정 CPU에서의 경합과 한 Hot Key의 배치·포화 여부를 구별하고, Source Partition 제약을 별도 다룬다. 재현성 점검은 [reproducibility](reproducibility.md), 실제 완료 판정은 [validation](validation.md)을 따른다.
