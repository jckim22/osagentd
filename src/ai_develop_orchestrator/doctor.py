from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH
from .launcher import _resolve_codex_bin


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check osagentd local runtime health")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--session-name", default="osagentd")
    parser.add_argument("--codex-bin", default=os.environ.get("AIDO_CODEX_BIN", "codex"))
    return parser.parse_args()


def _ok(name: str, detail: str = "") -> None:
    print(f"[ok]   {name}{': ' + detail if detail else ''}")


def _warn(name: str, detail: str = "") -> None:
    print(f"[warn] {name}{': ' + detail if detail else ''}")


def _fail(name: str, detail: str = "") -> None:
    print(f"[fail] {name}{': ' + detail if detail else ''}")


def _tmux_has(session_name: str, target: str = "") -> bool:
    full = f"{session_name}:{target}" if target else session_name
    return subprocess.run(
        ["tmux", "has-session", "-t", full],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    failures = 0

    if shutil.which("tmux"):
        _ok("tmux", shutil.which("tmux") or "")
    else:
        _fail("tmux", "install tmux first")
        failures += 1

    codex_bin = _resolve_codex_bin(args.codex_bin)
    if Path(codex_bin).exists():
        _ok("codex", codex_bin)
    else:
        _fail("codex", "set AIDO_CODEX_BIN or install the Codex CLI")
        failures += 1

    try:
        snapshot = request(args.socket_path, {"action": "status"})
    except Exception as exc:
        _warn("daemon", f"not reachable at {args.socket_path}: {exc}")
    else:
        metrics = snapshot.get("metrics", {})
        _ok(
            "daemon",
            f"agents={metrics.get('agent_count', 0)} queued={metrics.get('queued_tasks', 0)} running={metrics.get('running_tasks', 0)}",
        )

    if shutil.which("tmux") and _tmux_has(args.session_name):
        _ok("tmux session", args.session_name)
        windows = subprocess.run(
            ["tmux", "list-windows", "-t", args.session_name, "-F", "#{window_name}:#{window_panes}"],
            text=True,
            capture_output=True,
            check=False,
        ).stdout.strip()
        if windows:
            print(windows)
    else:
        _warn("tmux session", f"{args.session_name} is not running")

    latest = Path(".osagentd/runs/LATEST.md")
    if latest.exists():
        _ok("latest result", str(latest.resolve()))
    else:
        _warn("latest result", "no FINAL.md generated yet")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
