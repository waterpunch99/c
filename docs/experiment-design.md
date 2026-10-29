# Phase 3 실험 설계와 재현

구현 범위는 실험1/2다. Kafka32 Partition, Uniform User, 독립 round-robin Partition, Payload256, maxP128, Chaining off, HashMap/Checkpoint를 기본으로 고정한다. Heavy20000/Keyed1000은 Heavy 후보를 관측하기 위한 선택이며 병목은 실제 메트릭으로 확인한다. 수면으로 CPU를 흉내 내지 않는다. Docker CPU/Memory 예산은 기존 Phase2와 같다.

## 실험1

각 P의 신규 Run에서 낮은 Rate를 시작하고 모두 Stable이면 증가한다. 최초 과부하면 Rate를 낮추고, Stable/Backlog-Unstable bracket이 생기면 이분 탐색한다. 시도 횟수/최소·최대Rate/해상도는 고정 설정이다. INVALID/ERROR/UNKNOWN/반복 충돌이면 해당 P 탐색을 멈추고 불확실성을 보존한다. 검색 상한에서 Stable해도 RIGHT_CENSORED로 기록하며 절대 최대라 부르지 않는다. 처음부터 stable 하한이 없으면 NO_STABLE_LOWER_BOUND다.

이후 모든 P에 동일 Fixed Rate를 제공한다. 발견한 최대 순간 처리량을 Sustainable Throughput으로 바꾸지 않는다. Knee는 충분한 bracket 해상도가 있는 인접 P의 하한 개선율이 사전 기준보다 작은 경우 후보로만 표시하며 존재를 가정하지 않는다. 물리 CPU/Producer/Kafka 경합 해석은 별도다.

## 실험2

새 Calibration P=2/4/8 탐색으로 조건부 Operator Capacity 근거를 확인한다. 근거를 만족하면 고정 prediction.json으로 P를 계산하고 새 Seed/새 Run에서 주변 P를 Peak Rate로 검증한다. 계산 근거가 없으면 Full Validation은 생략하고 사유를 저장한다. Smoke는 계산 없이 사전 P의 독립 Peak probe를 수행한다. 모든 Calibration Run ID와 실제 사용한 Capacity 근거 Run ID를 각각 남긴다. 설명은 capacity-calculation.md를 따른다.

## 반복·분석

Full 조건당3회 이상, 기본 Warm-up60/Measurement180초다. 반복 Seed는 base+repeat, Validation은 여기에100000을 추가한다. 랜덤 순서는 order_seed로 재현한다. 적응형 탐색의 P 순서를 섞되 같은 P의 순차 Rate 결정을 유지하며 Fixed/Validation 후보는 반복 쌍을 섞는다. 실제 순서는 runs.jsonl/search.jsonl에 보존한다. 다른 Job이 있으면 Phase2 단일 Run이 거부하고 cleanup 실패면 다음 Run을 시작하지 않는다.

개별 Run Raw는 그대로 보존한다. Summary 통계는 역할/P/목표Rate/mode를 구분하고 VALID Run만 사용한다. 유효·실제 값 개수와 Stable 반복 개수를 같이 저장하며 1회 표본 Std는 결측이다. Latency Run quantile 평균을 전체 합동 p99라고 표시하지 않는다. 시간별 Histogram 차분/표본 수/미처리 코호트 완료율도 제공한다. PNG는 관측값이 있는 시리즈만 그리며 결측 차트는 Not Executed다. 한국어 리포트 8개 절은 질문/가설/환경/지표/실제 결과/관측 해석/실무 기준/한계를 구분한다.

## Full 실행 비용과 미검증

`python3 scripts/run_experiment.py --experiment 1 --mode full --dry-run`은 실행 없는 계획 확인이다. 기본 최대135 Run(120 탐색+15 Fixed), Load/Drain 예산41850초이며 Setup/API/정리 시간이 추가된다. 실험2는 최대87 Run(72 Calibration+15 Validation), 같은 예산26970초다. 실제 탐색은 불확실성/범위 해상도에 따라 일찍 멈출 수 있다. Full을 자동 실행하지 않는다. 명시적으로 시작할 경우 README 명령으로 실행하고 실제 데이터가 나온 후 분석한다.

Unit Fixture는 임시 폴더에서만 동작하며 results/에 예시 성능 데이터를 만들지 않는다. Smoke는 전체 성능 측정이 아니다. Operator별 Scale-out·Data Skew 비교·Partition 제한·전체 실험 all CLI는 다음 Phase로 남긴다.
