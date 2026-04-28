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
- `src/ai_develop_orchestrator/codex_worker.py`: 실제 Codex CLI 기반 워커
- `src/ai_develop_orchestrator/autoscaler.py`: 동적 worker 증감
- `src/ai_develop_orchestrator/submitter.py`: 작업 입력 pane
- `src/ai_develop_orchestrator/sample_agent.py`: 샘플 워커 에이전트
- `src/ai_develop_orchestrator/state.py`: 작업/에이전트/리소스 상태 관리
- `docs/ARCHITECTURE.md`: 구조 설명

## 제품형 빠른 시작

```bash
cd /home/brahe/Desktop/develop/ai_develop_orchestrator
./osagentd up
```

`submitter` pane에 자연어로 일을 적고 Enter를 누릅니다.

```text
task> ccsw_pro 프로그램 구조 분석하고 문서로 정리해줘
```

작업을 넣으면 `workers` 창으로 자동 전환되고, 역할별 pane이 실시간으로 보입니다.

```text
planner + researcher  -> 먼저 병렬 실행
coder                 -> plan/research 결과를 받아 실행
reviewer              -> code 결과를 받아 검토
merge                 -> 전체 결과를 통합해 FINAL.md 저장
```

결과 확인:

```bash
./osagentd results
```

터미널 UI를 열지 않고 바로 작업만 제출:

```bash
./osagentd ask "현재 프로젝트 구조 분석하고 개선 계획 세워줘"
```

`ask`는 daemon이 꺼져 있으면 백그라운드 tmux 세션을 자동으로 띄운 뒤 제출합니다.

최근 run 목록:

```bash
./osagentd runs
```

최신 최종 리포트 경로만 출력:

```bash
./osagentd latest
```

로컬 런타임 점검:

```bash
./osagentd doctor
```

실패 지점부터 재시도:

```bash
./osagentd retry
```

시간/토큰/병렬 효율 확인:

```bash
./osagentd metrics
```

최종 리포트는 항상 여기에 저장됩니다.

```text
.osagentd/runs/<run-id>/FINAL.md
.osagentd/runs/LATEST.md
```

완전히 다 끄려면:

```bash
./osagentd down
```

이미 켜져 있는 화면으로 다시 들어가려면:

```bash
./osagentd attach
```

구성:

- `control` window: monitor, daemon, submitter, autoscaler
- `workers` window: planner, researcher, coder, reviewer, merge worker
- `Ctrl-b n`: workers 창으로 이동
- `Ctrl-b p`: control 창으로 복귀

기본 동작:

- submitter pane에 자연어 한 줄 입력 후 Enter
- 터미널 UI 없이 쓰고 싶으면 `./osagentd ask "..."`
- 기본값은 `pipeline on`
- 한 요청이 `planner`, `researcher`, `coder`, `reviewer`, `merge` 작업으로 나뉩니다.
- downstream worker prompt에는 upstream 결과가 daemon에 의해 자동 삽입됩니다.
- autoscaler가 필요한 worker pane만 만들고, idle worker는 자동으로 내립니다.
- workers 창을 실수로 닫아도 autoscaler가 자동 복구합니다.
- 왼쪽 monitor의 `RUNS` 영역에서 역할 분담과 dependency 상태를 볼 수 있습니다.
- `doctor`, `runs`, `latest`, `results` 명령으로 상태와 산출물을 빠르게 확인합니다.
- `retry`로 실패 task와 downstream task만 다시 실행합니다.
- `metrics`로 stage별 런타임, 토큰 추정치, 병렬화 상한을 봅니다.

옵션 예시:

```bash
./osagentd up --monitor-width 45 --min-workers 0 --max-workers 6
```

Codex 비용 없이 UI만 테스트하려면:

```bash
./osagentd echo
```

참고:

- 이 모드는 `tmux`가 설치되어 있어야 합니다.
- 실시간 모니터만 단독 실행하려면 아래처럼 쓸 수 있습니다.
- Codex worker는 시스템에 설치된 `codex` CLI를 사용합니다.

```bash
PYTHONPATH=src python3 -m ai_develop_orchestrator.monitor
```

작업 입력은 `submitter` pane 안에서 합니다.

- 그냥 문장을 치고 Enter: 새 Codex 작업 제출
- `/pipeline on`
- `/pipeline off`
- `/target ccsw_pro`
- `/targets`
- `/caps code,python`
- `/resources src/app.py,README.md`
- `/workdir /home/brahe/Desktop/develop/ccsw_pro`
- `/priority 80`
- `/labels backend,urgent`
- `/show`
- `/quit`

## 저수준 CLI

daemon만 직접 띄우거나 외부 agent를 붙이는 개발자용 명령도 남아 있습니다.

```bash
PYTHONPATH=src python3 -m ai_develop_orchestrator.daemon
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
