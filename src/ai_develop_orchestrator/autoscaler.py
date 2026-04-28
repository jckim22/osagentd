from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import time
from typing import Dict, Iterable, List, Tuple

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH

ROLE_POOL = [
    ("planner", "plan,analysis,code"),
    ("researcher", "research,analysis,code"),
    ("coder", "code,python"),
    ("reviewer", "review,code"),
]
ROLE_CAPS = {role: _caps for role, _caps in ROLE_POOL}


def _cap_set(raw: str) -> set[str]:
    return {item.strip() for item in raw.split(",") if item.strip()}


def _role_for_task(task: Dict[str, object] | None, fallback_index: int) -> Tuple[str, str]:
    if not task:
        return ROLE_POOL[fallback_index % len(ROLE_POOL)]
    required = set(task.get("required_capabilities", []))
    for role, caps in ROLE_POOL:
        if required.issubset(_cap_set(caps)):
            return role, caps
    return "coder", "code,python"


def _covers(required: set[str], offered: Iterable[str]) -> bool:
    return required.issubset(set(offered))


def _pane_capabilities(pane_title: str) -> set[str]:
    role = pane_title.split("-", 1)[0]
    return _cap_set(ROLE_CAPS.get(role, ""))


def _task_is_covered(task: Dict[str, object], worker_agents: List[Dict[str, object]], worker_panes: List[Dict[str, str]]) -> bool:
    required = set(task.get("required_capabilities", []))
    if not required:
        return True
    for agent in worker_agents:
        if _covers(required, agent.get("capabilities", [])):
            return True
    for pane in worker_panes:
        if _covers(required, _pane_capabilities(pane["pane_title"])):
            return True
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dynamic tmux worker autoscaler for osagentd")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--session-name", required=True)
    parser.add_argument("--worker-window", default="workers")
    parser.add_argument("--workdir", default=os.getcwd())
    parser.add_argument("--pythonpath", default=os.environ.get("PYTHONPATH", "src"))
    parser.add_argument("--min-workers", type=int, default=0)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--idle-seconds", type=int, default=30)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--agent-capabilities", default="code,python")
    parser.add_argument("--codex-bin", default=os.environ.get("AIDO_CODEX_BIN", "codex"))
    parser.add_argument("--codex-model", default=os.environ.get("AIDO_CODEX_MODEL", ""))
    parser.add_argument("--executor-mode", choices=["codex", "echo"], default=os.environ.get("AIDO_EXECUTOR_MODE", "codex"))
    parser.add_argument("--bypass-codex-sandbox", action="store_true")
    return parser.parse_args()


def _run(cmd: List[str]) -> None:
    subprocess.run(cmd, check=True)


def _capture(cmd: List[str]) -> str:
    result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def _window_exists(session_name: str, window_name: str) -> bool:
    return subprocess.run(
        ["tmux", "list-panes", "-t", f"{session_name}:{window_name}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def _ensure_worker_window(args: argparse.Namespace) -> None:
    if _window_exists(args.session_name, args.worker_window):
        return
    _run(
        [
            "tmux",
            "new-window",
            "-d",
            "-t",
            args.session_name,
            "-n",
            args.worker_window,
            "bash",
            "-lc",
            "printf 'osagentd workers window recovered\\n\\nWorkers will appear here.\\n'; exec bash",
        ]
    )
    _run(["tmux", "select-pane", "-t", f"{args.session_name}:{args.worker_window}.0", "-T", "worker-board"])
    print(f"autoscaler recovered missing worker window: {args.worker_window}", flush=True)


def _pane_rows(session_name: str, window_name: str) -> List[Dict[str, str]]:
    result = subprocess.run(
        [
            "tmux",
            "list-panes",
            "-t",
            f"{session_name}:{window_name}",
            "-F",
            "#{pane_id}|#{pane_index}|#{pane_title}|#{pane_current_command}",
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    rows: List[Dict[str, str]] = []
    for line in result.stdout.splitlines():
        pane_id, pane_index, pane_title, pane_command = (line.split("|", 3) + ["", "", "", ""])[:4]
        rows.append(
            {
                "pane_id": pane_id,
                "pane_index": pane_index,
                "pane_title": pane_title,
                "pane_current_command": pane_command,
            }
        )
    return rows


def _worker_panes(args: argparse.Namespace) -> List[Dict[str, str]]:
    role_prefixes = tuple(f"{role}-" for role, _caps in ROLE_POOL)
    return [
        pane for pane in _pane_rows(args.session_name, args.worker_window)
        if pane["pane_title"].startswith(role_prefixes)
    ]


def _tmux_cmd(workdir: str, pythonpath: str, python_cmd: str, *, keep_open: bool = False) -> str:
    line = f"cd {shlex.quote(workdir)} && PYTHONPATH={shlex.quote(pythonpath)} {python_cmd}"
    if keep_open:
        line = (
            f"{line}; status=$?; "
            "echo; "
            "echo '================================================================'; "
            "echo \"[osagentd] worker process exited with status ${status}\"; "
            "echo '[osagentd] pane is kept open so the error/output does not disappear.'; "
            "echo '================================================================'; "
            "exec bash"
        )
    return f"bash -lc {shlex.quote(line)}"


def _worker_command(args: argparse.Namespace, worker_name: str, capabilities: str) -> str:
    base = (
        "python3 -m ai_develop_orchestrator.codex_worker "
        f"--name {shlex.quote(worker_name)} "
        f"--socket-path {shlex.quote(args.socket_path)} "
        f"--workdir {shlex.quote(args.workdir)} "
        f"--capabilities {shlex.quote(capabilities)} "
        f"--codex-bin {shlex.quote(args.codex_bin)} "
        f"--executor-mode {shlex.quote(args.executor_mode)} "
    )
    if args.codex_model:
        base += f"--codex-model {shlex.quote(args.codex_model)} "
    if args.bypass_codex_sandbox:
        base += "--bypass-codex-sandbox "
    return _tmux_cmd(args.workdir, args.pythonpath, base, keep_open=True)


def _spawn_worker(args: argparse.Namespace, worker_name: str, capabilities: str) -> None:
    command = _worker_command(args, worker_name, capabilities)
    board_pane = _worker_board_pane(args)
    if board_pane:
        pane_id = board_pane
        _run(["tmux", "respawn-pane", "-k", "-t", pane_id, command])
    else:
        pane_id = _capture(
            [
                "tmux",
                "split-window",
                "-d",
                "-P",
                "-F",
                "#{pane_id}",
                "-t",
                f"{args.session_name}:{args.worker_window}.0",
                command,
            ]
        )
    _run(["tmux", "select-layout", "-t", f"{args.session_name}:{args.worker_window}", "tiled"])
    _run(["tmux", "select-pane", "-t", pane_id, "-T", worker_name])
    print(f"autoscaler spawn {worker_name} caps={capabilities} pane={pane_id}", flush=True)


def _worker_board_pane(args: argparse.Namespace) -> str:
    for pane in _pane_rows(args.session_name, args.worker_window):
        if pane["pane_title"] == "worker-board":
            return pane["pane_id"]
    return ""


def _kill_worker_pane(args: argparse.Namespace, worker_name: str) -> None:
    for pane in _pane_rows(args.session_name, args.worker_window):
        if pane["pane_title"] == worker_name:
            _run(["tmux", "kill-pane", "-t", pane["pane_id"]])
            _run(["tmux", "select-layout", "-t", f"{args.session_name}:{args.worker_window}", "tiled"])
            return


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    if shutil.which("tmux") is None:
        print("tmux not found", file=sys.stderr)
        return 2

    last_busy: Dict[str, float] = {}
    next_worker_index = 1
    while True:
        try:
            _ensure_worker_window(args)
            snapshot = request(args.socket_path, {"action": "status"})
            agents = snapshot.get("agents", [])
            tasks = snapshot.get("tasks", [])
            queued = [task for task in tasks if task.get("status") == "queued"]
            worker_agents = [a for a in agents if a.get("metadata", {}).get("worker_type") == "codex"]
            worker_panes = _worker_panes(args)
            running = [a for a in worker_agents if a.get("current_task_id")]
            idle = [a for a in worker_agents if not a.get("current_task_id")]

            now = time.time()
            for agent in worker_agents:
                name = agent.get("name", "")
                if agent.get("current_task_id"):
                    last_busy[name] = now
                else:
                    last_busy.setdefault(name, now)

            desired = min(args.max_workers, max(args.min_workers, len(queued) + len(running)))
            current = max(len(worker_agents), len(worker_panes))

            spawned_this_round = 0
            for task in queued:
                if current >= args.max_workers:
                    break
                if _task_is_covered(task, worker_agents, worker_panes):
                    continue
                role, role_caps = _role_for_task(task, next_worker_index - 1)
                worker_name = f"{role}-{next_worker_index}"
                next_worker_index += 1
                _spawn_worker(args, worker_name, role_caps)
                worker_panes.append({"pane_id": "", "pane_index": "", "pane_title": worker_name, "pane_current_command": ""})
                current += 1
                spawned_this_round += 1

            while current < desired:
                task_hint = queued[min(spawned_this_round, len(queued) - 1)] if queued else None
                role, role_caps = _role_for_task(task_hint, next_worker_index - 1)
                worker_name = f"{role}-{next_worker_index}"
                next_worker_index += 1
                _spawn_worker(args, worker_name, role_caps)
                current += 1
                spawned_this_round += 1

            removable = sorted(
                [agent for agent in idle if len(worker_agents) > args.min_workers],
                key=lambda item: last_busy.get(item.get("name", ""), now),
            )
            for agent in removable:
                if len(worker_agents) <= desired or len(worker_agents) <= args.min_workers:
                    break
                name = agent.get("name", "")
                if now - last_busy.get(name, now) < args.idle_seconds:
                    continue
                request(args.socket_path, {"action": "unregister", "agent_id": agent.get("agent_id")})
                _kill_worker_pane(args, name)
                print(f"autoscaler stop idle worker {name}", flush=True)
                worker_agents = [item for item in worker_agents if item.get("name") != name]

            print(
                f"autoscaler agents={len(worker_agents)} panes={len(worker_panes)} queued={len(queued)} running={len(running)} desired={desired}",
                flush=True,
            )
            time.sleep(max(0.5, args.poll_seconds))
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            print(f"autoscaler error: {exc}", flush=True)
            time.sleep(max(0.5, args.poll_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
