# Architecture

## Control Plane

모든 에이전트는 오케스트레이터 데몬과만 통신합니다.

1. 에이전트가 `register`
2. 작업 producer가 `submit-task`
3. 에이전트가 `poll-task`
4. 데몬이 capability, priority, resource availability를 보고 할당
5. 완료 시 `complete-task`

## Resource Model

- 리소스는 파일, 디렉터리, 브랜치, worktree, 장비 이름 등 문자열 토큰
- 모든 토큰은 정규화되고 정렬된 뒤 락 획득
- 실제 락은 데몬만 `flock`으로 보유
- 에이전트는 lease만 위임받음

## Deadlock Avoidance

- 에이전트가 직접 락을 잡지 않음
- 데몬이 항상 정렬된 순서로만 리소스를 획득
- atomic multi-resource acquire만 허용

## Failure Recovery

- 각 에이전트는 heartbeat를 보냄
- timeout 시 stale agent로 판단
- 보유 lease와 실행 중 task를 회수
- 작업은 다시 queue로 되돌리거나 failed 처리 가능

## Recommended Production Topology

- `orchestrator daemon`: systemd service
- `agent wrapper`: Codex/worker 프로세스 앞단
- `task producer`: CLI, API, UI, scheduler
- `state file`: 운영 디버깅용

