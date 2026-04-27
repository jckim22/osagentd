# osagentd

Codex 같은 개별 에이전트가 서로 직접 조율하지 않고, 로컬 OS 위에서 동작하는 별도 데몬이 작업 배분과 리소스 락을 맡는 OS-native 멀티에이전트 런타임입니다.

## 핵심 아이디어

- 오케스트레이터는 에이전트가 아니라 `OS daemon`
- 에이전트들은 `Unix domain socket`으로 데몬과만 통신
- 파일/리소스 교착 상태는 중앙 데몬이 `flock(2)` 기반 lease로 해결
- 작업은 capability와 priority 기준으로 배분
- 에이전트 장애는 heartbeat timeout으로 감지하고 lease 자동 회수

## 프로젝트 구조

- `src/ai_develop_orchestrator/daemon.py`: 데몬 서버
- `src/ai_develop_orchestrator/cli.py`: 운영/에이전트용 CLI
- `src/ai_develop_orchestrator/sample_agent.py`: 샘플 워커 에이전트
- `src/ai_develop_orchestrator/state.py`: 작업/에이전트/리소스 상태 관리
- `docs/ARCHITECTURE.md`: 구조 설명

## 빠른 시작

```bash
cd /home/brahe/Desktop/develop/ai_develop_orchestrator
PYTHONPATH=src python3 -m ai_develop_orchestrator.daemon
```

다른 터미널에서:

```bash
PYTHONPATH=src python3 -m ai_develop_orchestrator.cli register \
  --name planner \
  --capabilities plan,code \
  --metadata '{"role":"planner"}'
```

작업 제출:

```bash
PYTHONPATH=src python3 -m ai_develop_orchestrator.cli submit-task \
  --task-type patch \
  --priority 100 \
  --required-capabilities code \
  --required-resources repo/app.py,repo/README.md \
  --payload '{"goal":"refactor api layer"}'
```

샘플 에이전트 실행:

```bash
PYTHONPATH=src python3 -m ai_develop_orchestrator.sample_agent \
  --name worker-a \
  --capabilities code,python
```

상태 조회:

```bash
PYTHONPATH=src python3 -m ai_develop_orchestrator.cli status
```

## 기본 파일

- socket: `/tmp/ai_develop_orchestrator.sock`
- state: `/tmp/ai_develop_orchestrator_state.json`
- leases: `/tmp/ai_develop_orchestrator_locks`

## 다음 확장 포인트

- Git worktree 단위 리소스 분리
- Codex wrapper 프로세스 자동 등록
- systemd unit 배포
- 웹 대시보드
- 정책 기반 preemption
