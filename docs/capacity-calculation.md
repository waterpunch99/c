# Capacity 기반 병렬도 계산

## 공식과 범위

`P_i = ceil(lambda_i / (C_i * U_i))`. lambda는 해당 Operator 예상 입력률이다. 핵심 Pipeline은1:1이므로 Job Peak와 같지만 축약/증폭 Operator에는 별도 lambda가 필요하다. U는 Capacity 대비 목표 비율이며 CPU 사용량이나 Flink Busy와 같지 않다. calculate_parallelism.py는 Operator별 입력도 지원하며 숫자 계산 자체는 Capacity를 측정하지 않는다. 40000/7000/0.7 → P9는 설명용 예이며 실측이 아니다. 부하/Subtask, Capacity 활용률, Headroom을 함께 출력한다.

## 자동 예측 근거

실험2는 독립 Calibration에서 P별 Rate 탐색을 수행한다. 모든 반복이 VALID+STABLE인 Rate는 하한, 모든 반복이 VALID+UNSTABLE이고 실제 backlog 증가가 이유인 Rate만 상한이다. Checkpoint 실패만으로 상한을 만들지 않는다. 상한/하한 구간이 충분히 좁아야 한다.

Operator의 실제 Subtask Counter 차분을 측정 sample 시간으로 나눠 처리율을 계산한다. Source/Parse/Heavy/Aggregation은 numRecordsOut, Sink는 completedRecords를 사용한다. Counter reset/결측/배치 변화는 Capacity 근거를 무효로 한다. Job 처리량/P를 모든 Operator 용량으로 쓰지 않는다.

하한과 상한의 각 Run에서 Busy가 높고 Backpressure는 낮으며 Subtask 처리율이 비교적 균등하고 다른 Operator와 Busy 차이가 있는 유일 CPU/stateful 후보가 있어야 한다. Source/Sink 후보나 애매한 후보는 자동 예측하지 않는다. 이는 병목 후보의 관측 근거이며 Busy가 CPU라는 뜻은 아니다.

실제 TM/Kafka/Producer quota 포화·Throttle·필수 리소스 결측이 있으면 다른 P로 전이 가능한 용량을 추정하지 않는다. 고정 자원의 포화가 용량을 제한할 수 있기 때문이다. 복수 P의 보수적인 직접 Operator/Subtask 처리율 추정치를 비교하고 편차가 허용 범위 이내일 때만 최소값을 조건부 Capacity로 선택한다. 환경·Artifact·Workload fingerprint도 같아야 한다. 임계값은 phase3.json에 사전 등록한다.

값이 없으면 `NOT_ESTIMATED`이며 대체 상수·Job/P·Smoke Rate/P를 넣지 않는다. 기본 로컬 TM2 CPU와 Kafka1 CPU 환경에서는 이 거부가 정상적인 결과일 수 있다. 엄격한 조건을 결과에 맞춰 낮추지 않는다. 처리량 자체는 표에 보존한다.

## 독립 검증과 평가

예측 JSON은 Validation 시작 전에 exclusive로 저장한다. 검증 후보는 P*-2/P*-1/P*/P*+1/P*+3의 양수 중복 없는 집합이며 Slot/최대P 예산 밖은 Not Executed다. 전체 Operator를 동일 P로 적용한다. 이번 Phase의 Operator별 산술 API는 있으나 Operator별 선택 Scale-out 비교는 실험3 후속 범위다.

Calibration/Validation은 새 Topic/Group/Job/State/Run을 사용한다. Workload/분포는 유지하고 Validation Seed에100000을 더한다. 조건/자원 fingerprint가 같고 반복 모두 VALID+STABLE인 실제 후보 중 최소값만 Measured Minimum Stable P로 표시한다. 전역 최소를 찾았다고 주장하지 않는다. Signed Calculation Error는 predicted-measured, Relative Error는 이를 measured로 나눈 값이다.

운영 추천 P는 Stable 이외에 p99 SLO, 최신 완료 Checkpoint duration, 실제 TM CPU/Throttle와 Capacity 대비 목표 Headroom을 만족한 후보 중 최소다. 이 추천은 설정한 운영 정책에만 유효하다. SLO 기본값은 Flink 표준이 아니며 실험용 기준이다. U=0.6/0.7/0.8 민감도도 예측 근거가 있을 때 계산한다.

## Smoke 해석

Smoke는 지속 안정성이 UNKNOWN이므로 측정 Capacity/P 예측을 만들지 않는다. Calibration 뒤 별도 사전 지정 P의 Peak probe를 실행해 독립 실행 경로를 검증한다. 해당 probe는 계산값의 실측 검증이 아니다. 실제 Full은 이번 요청에서 실행하지 않으며 공식·근거 거부·범위 축소·예측 고정·주변 후보·반복 최소·운영 평가를 TemporaryDirectory Fixture 테스트로 검증한다. 실제 Full 결과와 Fixture를 혼합하지 않는다.
