# Flink Parallelism은 어떤 기준으로 결정해야 하는가?

이 문서는 블로그의 분석 틀과 실제 검증의 입구다. Phase5에서 실험1~5 전체 축소 Smoke와 실험5/1 수정 후 독립 Smoke를 실행했다. [최신 선택 통합 리포트](../results/reports/report_a306f3d0e0c448168054d0a299989136/blog-report.md)는 실제14 Run을 링크하며, [원래 Suite의 실패 포함 리포트](../results/reports/report_4688e9520cc2467298adf684b20f961e/blog-report.md)는11 VALID/2 INVALID/1 ERROR를 보존한다. 두 선택은 동일한 Suite가 아니다. Session별 analysis_UUID/report.md는 실측 표/PNG를 제공한다. 장시간 Sustainable Capacity/계산한 P/포화 병목·성능 개선의 Full 검증은 **Not Executed**다. 상세 이력은 [validation](validation.md)을 따른다.

## 1. 실험 질문

측정한 Subtask Capacity로 계산한 Parallelism이 실제 Peak 부하에 적절한가? 고정 CPU/메모리와 Hot Key가 그 예측을 어떻게 제한하는가?

## 2. 가설

가설: 병목 Subtask의 측정 Capacity와 균등한 입력 분포가 유지되는 조건에서 계산식이 유용한 첫 추정일 수 있다. 고정 물리 자원 경합이나 Hot Key에서는 단순 선형 예측이 어긋날 수 있다. 가설은 측정 결과가 아니며 반증된 결과도 그대로 기록한다.

## 3. 실험 환경

설계는 architecture.md, 실제 Smoke 비교의 환경은 해당 Run의 metadata.json/environments.json에 기록한다. 장시간 Full 성능 비교는 **Not Executed**다.

## 4. 측정 지표

생산 Target/Sent/ACK와 Sink 완료율, Backlog 회귀·누적 변화, 생성 코호트 Latency, Subtask Busy/Idle/Backpressure, CPU quota/Throttle, Checkpoint/Restart, Partition·Key 분포. Source/Sink/Producer 선포화와 Aggregation Skew를 구분하기 위해 함께 사용한다.

## 5. 실험 결과

Phase5 최종 분석은 실험1/2/3/4/5 각각2/2/4/2/4 Run, 모두 VALID+UNKNOWN인 최신 완료 Session을 선택했다. 실험5는 별도20 records/sec 조건이며 실험1~4 및 기존100 records/sec Source Run과 성능 통계를 합치지 않는다. 각 실험의 실측 PNG는6/6/15/14/13개, 총54개다. 미측정 Sustainable Capacity/Predicted P 차트는 Not Executed다. 분석 스크립트/원본/선택 Hash는 각 analysis.json/report.json에 기록했다.

실험5의 Kafka8 Partition 실제 Reader별 Count는 P2=[4,4], P4=[2,2,2,2], P8=[1×8], P16=[0×5,1×8,0×3]이었다. 모든 Run은 측정 생성/Sent/ACK200건, 시간 내 코호트 완료195/196/196/195건, Drain5/4/4/5건, 최종 미처리0건이다. P16 미할당 Reader는 이 Run의0~4와13~15였고 할당은 실제 수락 콜백과 전체 Partition 소유자로 확인했다. 아래 표는 삭제하지 않은 **이전 Phase4 종료 시점의 기록**이다.

| 실험 | 결과 | 그래프 |
|---|---|---|
| 1 Parallelism / Capacity | Smoke 4 Run VALID + UNKNOWN, Capacity Not Executed | 실측 Smoke PNG6개 생성 |
| 2 Capacity 계산 / 독립 검증 | Smoke 4 Run VALID + UNKNOWN, 측정 Capacity 예측 Not Executed | 실측 probe PNG6개 생성 |
| 3 Operator별 병목 | Smoke4 Run VALID + UNKNOWN, 포화 후보 미관측/사전점검 불확실 | 실측 PNG15개 생성 |
| 4 Uniform / Skewed | Smoke4 Run VALID + UNKNOWN, Kafka TV0/실제 Hot 단일 배치 관측, 포화 후보 미관측 | 실측 PNG14개 생성 |
| 5 Partition / Source P | Not Executed | Not Executed |

## 6. 원인 분석

실험5에서 Source P가 Partition 수보다 클 때 실제 할당이 없는 Reader를 확인했고, Downstream P8과 Source P를 독립 설정해 실행했다. 이 저부하에서 Source 포화 후보는 없었으므로 Source Capacity 또는 P8 이후의 처리량 효과는 검증되지 않았다. 각 Source Run의 Latency 표본20건, 수집4~5회, 초기 Checkpoint 실패1~2회와 리소스 경고를 포함한다. P4의 구간 Coverage59.62%와 resource_evidence_missing도 보존하며 운영 안정성 증명으로 쓰지 않는다. 이전100 records/sec 두 INVALID Run의 할당은 독립 증거로 표시하고 성능 비교에서는 제외했다.

실험4 P2/4에서 실제 Hot 코호트394건(39.4%) 모두 각각 Subtask1/2에 남고 Key Group86을 관측했다. Kafka Partition 쌍의 실제 ACK 비율 TV0이었다. 처리 코호트 Max/Mean은 Uniform P2/4=1.004/1.068, Skewed=1.388/2.188로 관측됐다. 전체 코호트1000건·Hot394건이 생성/ACK/처리 Counter와 일치했다.

이는 측정한 실행의 Key 부하 집중 증거이며 포화 성능 병목이나 Capacity 감소의 증명은 아니다. 저부하 Smoke의 의도한 병목 후보는 없었다. 실험3의 A/B/C Sink 측정 코호트 처리율97.8/99.3/98.0 records/sec는 짧은 경계·초기 메트릭·JIT·1회 반복의 영향을 분리하지 못했다. 가설에 맞춰 수치를 변경하지 않았다. Busy는 CPU가 아니며 CPU 샘플은 동기 연산 구간 비용만 제공한다. Knee Point·최소 Stable P·계산 오차는 미관측이다.

## 7. 실무 적용 기준

아직 실측 기반 운영 추천은 없다. 후속 분석에서는 예측 P, 탐색 범위 내 최소 Stable P, Latency/Checkpoint/Headroom까지 만족하는 운영 P를 구분한다. `ceil(Peak / (Capacity × Target Utilization))`의 Target Utilization은 JVM CPU/Busy와 다르다.

## 8. 실험의 한계

fixed-resource에서 P를 높여도 CPU 예산은 늘지 않는다. 실제 실험3의 Uniform High/Focused occupied Slot은 모두8이고 Operator Subtask 합은40/18이었다. 이를 CPU/Slot 절감이라고 주장하지 않는다. 실험4에서는 한 Hot Key가 각 Run의 한 Subtask에 남았지만 P8/16 및 포화 성능은 아직 검증하지 않았다. Kafka Partition을 userId와 독립적으로 분산하므로 User별 처리는 도착 순서의 누적이며 Producer의 전역 eventId 순서를 보장하지 않는다. Phase1/짧은 Smoke를 Sustainable Capacity로 일반화하지 않는다. 상세 한계는 limitations.md를 따른다.
