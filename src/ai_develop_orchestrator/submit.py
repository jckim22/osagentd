from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH
from .submitter import _csv, _submit_one, _submit_pipeline


APP_DIR = Path(__file__).resolve().parents[2]


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit an osagentd task without opening the tmux UI")
    parser.add_argument("prompt", nargs="+")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--workdir", default=os.getcwd())
    parser.add_argument("--caps", default="code,python")
    parser.add_argument("--resources", default="")
    parser.add_argument("--labels", default="")
    parser.add_argument("--priority", type=int, default=50)
    parser.add_argument("--single", action="store_true", help="Submit one worker task instead of the default DAG pipeline")
    parser.add_argument("--session-name", default="osagentd")
    parser.add_argument("--no-autostart", action="store_true", help="Fail instead of starting osagentd when the daemon is down")
    return parser.parse_args(list(argv) if argv is not None else None)


def _daemon_alive(socket_path: str) -> bool:
    try:
        response = request(socket_path, {"action": "status"})
    except OSError:
        return False
    except RuntimeError:
        return False
    return bool(response.get("ok"))


def _wait_for_daemon(socket_path: str, timeout_seconds: float = 8.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _daemon_alive(socket_path):
            return True
        time.sleep(0.25)
    return False


def _autostart(args: argparse.Namespace) -> bool:
    if args.no_autostart:
        return False
    print("osagentd daemon is not running; starting background tmux session...", file=sys.stderr)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "ai_develop_orchestrator.launcher",
            "--app-dir",
            str(APP_DIR),
            "--workdir",
            str(APP_DIR),
            "--target-root",
            str(Path(args.workdir).expanduser().resolve().parent),
            "--session-name",
            args.session_name,
            "--socket-path",
            args.socket_path,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr or result.stdout)
        return False
    sys.stderr.write(result.stdout)
    return _wait_for_daemon(args.socket_path)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if not _daemon_alive(args.socket_path) and not _autostart(args):
        print(
            "osagentd daemon is not reachable. Run './osagentd up' or retry without --no-autostart.",
            file=sys.stderr,
        )
        return 1

    prompt = " ".join(args.prompt).strip()
    state = {
        "caps": _csv(args.caps),
        "priority": args.priority,
        "task_type": "codex-task",
        "labels": _csv(args.labels),
        "resources": _csv(args.resources),
        "workdir": args.workdir,
    }
    request_args = SimpleNamespace(socket_path=args.socket_path)
    if args.single:
        response = _submit_one(request_args, state, prompt)
        if not response.get("ok"):
            print(response)
            return 1
        print(f"submitted task {response.get('task_id')}")
        return 0

    run_parts = _submit_pipeline(request_args, state, prompt)
    print(f"submitted run {run_parts[0]}")
    print("tasks:")
    for task_id in run_parts[1:]:
        print(f"- {task_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
