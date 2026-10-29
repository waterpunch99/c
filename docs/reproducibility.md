# 재현 절차와 검토 범위

## 동일 조건 재실행

Repository 루트에서 실행한다. Docker Socket·Registry/Maven/PyPI 접근, 4CPU/약8GiB RAM이 필요하다. 기존 Phase 결과는 삭제하지 않는다.

```bash
docker compose config --quiet
docker compose run --rm builder -B -ntp verify
docker compose build producer analyzer
docker compose run --rm --entrypoint python analyzer -m unittest discover -s tests -v
docker compose up -d --wait --wait-timeout 180 kafka jobmanager taskmanager
python3 scripts/smoke_test.py
python3 scripts/run_experiment.py --experiment all --mode smoke --config configs/experiments/suite-quick-smoke.json --dry-run
python3 scripts/run_experiment.py --experiment all --mode smoke --config configs/experiments/suite-quick-smoke.json
```

마지막 CLI가 출력한 실제 `suite_directory`를 다음 명령의 `SUITE_DIRECTORY`에 넣는다.

```bash
docker compose run --rm analyzer --experiment all --mode smoke --suite SUITE_DIRECTORY
```

Host 경로는 컨테이너의 `/workspace/results/suites/suite_UUID` 또는 상대 `results/suites/suite_UUID`로 지정한다. 출력한 reports/report_UUID/blog-report.md와 report.json에서 선택된 모든 Session을 확인한다. 생성된 리포트/실측 Run을 보관한다.

개별 실험의 전체 Smoke 비교는 다음을 사용한다. all 기본 Smoke는20 Run이며 위 quick 설정은14 Run이다. 재실행 값이 같아야 한다는 요구가 아니라 조건/계측/해석이 재현 가능한지 확인한다.

```bash
python3 scripts/run_experiment.py --experiment 5 --mode smoke
python3 scripts/run_experiment.py --experiment 5 --mode smoke --target-rate 20
docker compose run --rm analyzer --experiment 5 --mode smoke
python3 scripts/run_experiment.py --experiment all --mode smoke --dry-run
```

Full의 계획 확인만으로 Full 검증을 완료했다고 표시하지 않는다. 실제 실행은 별도 명시적 요청/로컬 선택이 필요하다.

이번 Phase5에서는 기본100 records/sec Suite의 Producer expired/JSON 수집 실패를 그대로 보존하고, 수정한 Collector와20 records/sec 조건의 실험5를 별도 UUID에 실행했다. target-rate가 다르므로 기존 Run과 반복 통계를 합치지 않는다. 최신 Session 분석과 명시적 --suite 분석도 서로 다른 선택임을 report.json에서 확인한다.

```bash
python3 scripts/run_experiment.py --experiment all --mode full --dry-run
# 장시간 실행을 선택한 경우에만 위 명령에서 --dry-run을 제거
```

## 조건과 근거 보관

각 Run의 metadata.json: OS/Architecture·CPU affinity/quota·Docker inspect(이미지 ID/제한)·실제 Flink JVM/Slots/Graph/배치·Clock·Workload·구간·JAR/Producer/Controller SHA256·Git SHA. 최초 Commit이 없으면 Git SHA는 null이므로 코드 archive 및 Artifact Hash가 필요하다. Suite는 실행 전에 전체 frozen 조건/순서/비용/Artifact Hash를 보관한다. 실험2는 예측을 Validation 전에 저장하고 별도 Run/State/Topic에서 검증한다.

보관 대상은 전체 `results/experiment_N/session_UUID`, `results/suites/suite_UUID`, `results/reports/report_UUID`, 사용한 configs/Compose/소스/빌드 JAR/의존성 그래프다. reports의 외부 상대 링크를 유지하려면 results 트리 전체를 함께 복사한다. results는 gitignore이므로 일반 git clone에 실측 결과가 포함되지 않는다.

이미지는 구체 버전 태그이며 실제 image ID/Digest가 기록된다. 엄밀한 동일 바이너리가 필요하면 기록된 Digest로 Compose override를 만들고 Producer/analyzer 이미지도 archive한다. 이때 조건 fingerprint가 달라질 수 있어 새 Session으로 남긴다. Maven `dependency:tree`는 빌드 캐시가 준비된 상태에서 `docker compose run --rm builder -B dependency:tree`로 확인할 수 있다. 테스트 결과는 target/surefire-reports에 보존한다.

## 격리와 안전 정리

Run마다 Topic/Consumer Group/Job/신규 State/Checkpoint 경로/UUID 출력이 독립적이다. 다른 활성 Job은 실행을 거부한다. 종료 시 해당 Producer/Probe/Observer·Job terminal·Topic 삭제를 확인하며 cleanup_errors를 저장한다. Checkpoint volume/자료는 보존하고 restore하지 않는다. 전체 서비스 종료는 `docker compose down`이며 결과/볼륨 삭제를 포함하지 않는다.

INT/TERM은 기존 run_once finally 정리 경로로 전달된다. 제어기 프로세스의 SIGKILL/Host 종료는 finally를 실행하지 못한다. suite/run.json의 실제 Job/Topic/Container를 확인해 수동으로 해당 자원만 정리하고 다음 Run을 시작한다. 정리가 불확실하면 Suite는 다음 실험을 시작하지 않는다.

## 검토 한계

검증 환경은 로컬 단일 Broker/JM/TM, cgroupv2, Java17/Python3.12 컨테이너다. 다른 OS/ARM/cgroupv1·고부하 Producer·Full 반복/장시간 State 성장·외부 Sink·HA·장애복구·Watermark·Salting·정량 계측 오버헤드는 미검증 또는 미구현이다. Host와 모든 컨테이너는 CPU를 공유한다. 새 Source Gauge가 추가된 JAR와 이전 Phase JAR를 같은 바이너리라고 간주하지 않는다.

모든 Smoke는 Capacity/운영 P를 측정하는 Full 실험과 구분한다. Short Run에서 실제 할당과 Hot 배치는 검증할 수 있어도 처리 여력/인과 효과를 확정할 수 없다. Unit Fixture는 tests의 TemporaryDirectory 안에만 생성되며 results와 혼합하지 않는다. 실제 명령·결과·오류·완료 판정은 validation.md를 따른다.
