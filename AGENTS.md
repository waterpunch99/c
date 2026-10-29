# 개발 지침

- 현재 구현 범위와 전체 명세를 구분한다. 전체 요구사항은 `docs/benchmark-spec.md`, 단계별 완료 조건은 `docs/implementation-plan.md`를 따른다.
- 파일 수정 전 기존 코드와 Git 상태를 확인한다. 기존 결과나 사용자 작업을 삭제·덮어쓰지 않는다.
- 실제 데이터만 성능 결과로 보고한다. 테스트 Fixture는 실측 결과와 분리한다. 미실행은 `Not Executed`, 결측은 null과 원인으로 남긴다.
- Execution Validity와 Performance Stability를 독립적으로 기록한다. 정상적인 Backlog 증가는 오류가 아니다. VALID + UNSTABLE도 보존한다.
- 버전, 조건, 리소스 예산, Job ID, Run ID를 기록한다. Phase 완료는 실제 검증 근거로 판정한다.
- Full Benchmark는 명시적 실행 요청이 있을 때만 실행한다. Smoke는 Capacity 증명이 아니다.
- Job 처리량/P를 모든 Operator의 Capacity로 취급하지 않는다. Busy Time은 CPU Usage가 아니다.
- Source 미할당은 실제 할당 Count/ID와 전체 Partition 관측으로 확인한다. 읽기 기록 없음이나 예상 배치를 실측 Assignment로 바꾸지 않는다.
- 성능 Run 중에는 코드/JAR/Producer/Compose를 고정한다. Build/테스트/분석 부하는 측정 종료 후 실행한다.
- 변경 후 해당 범위의 Build와 테스트를 실행하고, 실패·차단·미검증을 `docs/validation.md`에 남긴다.
- 한국어 README와 재현 명령을 유지한다. 다음 Phase의 작업을 현재 구현 완료로 표시하지 않는다.
