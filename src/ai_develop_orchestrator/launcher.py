from __future__ import annotations

import argparse
import contextlib
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List

from .daemon import DEFAULT_LOCK_DIR, DEFAULT_SOCKET_PATH, DEFAULT_STATE_FILE


APP_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="tmux launcher for osagentd")
    parser.add_argument("--session-name", default="osagentd")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--state-file", default=os.environ.get("AIDO_STATE_FILE", DEFAULT_STATE_FILE))
    parser.add_argument("--lock-dir", default=os.environ.get("AIDO_LOCK_DIR", DEFAULT_LOCK_DIR))
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=int(os.environ.get("AIDO_HEARTBEAT_TIMEOUT_MS", "30000")))
    parser.add_argument("--monitor-width", type=int, default=35)
    parser.add_argument("--worker-window", default="workers")
    parser.add_argument("--app-dir", default=str(APP_DIR), help="Directory containing this osagentd project")
    parser.add_argument("--workdir", default=str(APP_DIR), help="Internal runtime working directory")
    parser.add_argument(
        "--target-root",
        default=str(APP_DIR.parent),
        help="Directory scanned by the submitter for target repositories",
    )
    parser.add_argument("--agent-capabilities", default="code,python")
    parser.add_argument("--min-workers", type=int, default=0)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--idle-seconds", type=int, default=30)
    parser.add_argument("--codex-bin", default=os.environ.get("AIDO_CODEX_BIN", "codex"))
    parser.add_argument("--codex-model", default=os.environ.get("AIDO_CODEX_MODEL", ""))
    parser.add_argument("--executor-mode", choices=["codex", "echo"], default=os.environ.get("AIDO_EXECUTOR_MODE", "codex"))
    parser.add_argument("--bypass-codex-sandbox", action="store_true", default=True)
    parser.add_argument("--attach", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--stop", action="store_true")
    return parser.parse_args()


def _run(cmd: List[str]) -> None:
    subprocess.run(cmd, check=True)


def _shell_line(app_dir: str, workdir: str, python_cmd: str) -> str:
    src_dir = str(Path(app_dir) / "src")
    return f"cd {shlex.quote(workdir)} && PYTHONPATH={shlex.quote(src_dir)} {python_cmd}"


def _tmux_shell_command(app_dir: str, workdir: str, python_cmd: str) -> str:
    return f"bash -lc {shlex.quote(_shell_line(app_dir, workdir, python_cmd))}"


def _split_window(cmd: List[str]) -> str:
    result = subprocess.run(cmd, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def _tmux_exists() -> bool:
    return shutil.which("tmux") is not None


def _resolve_codex_bin(raw: str) -> str:
    path = Path(raw).expanduser()
    if path.parent != Path("."):
        return str(path.resolve())
    found = shutil.which(raw)
    if found:
        return str(Path(found).resolve())
    candidates = sorted(Path.home().glob(".vscode-server/extensions/openai.chatgpt-*/bin/linux-x86_64/codex"))
    if candidates:
        return str(candidates[-1].resolve())
    return raw


def _session_exists(session: str) -> bool:
    result = subprocess.run(
        ["tmux", "has-session", "-t", session],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _kill_session(session: str) -> None:
    subprocess.run(
        ["tmux", "kill-session", "-t", session],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    if not _tmux_exists():
        print("tmux is required for launcher mode. Install tmux first.", file=sys.stderr)
        return 2

    session = args.session_name
    app_dir = str(Path(args.app_dir).expanduser().resolve())
    workdir = str(Path(args.workdir).expanduser().resolve())
    target_root = str(Path(args.target_root).expanduser().resolve())
    if args.stop:
        _kill_session(session)
        print(f"stopped session: {session}")
        return 0

    if not Path(app_dir).exists():
        print(f"osagentd app directory does not exist: {app_dir}", file=sys.stderr)
        return 2
    if not (Path(app_dir) / "src" / "ai_develop_orchestrator").exists():
        print(f"osagentd app directory is invalid: {app_dir}", file=sys.stderr)
        return 2
    if not Path(workdir).exists():
        print(f"runtime workdir does not exist: {workdir}", file=sys.stderr)
        return 2
    if not Path(target_root).exists():
        print(f"target root does not exist: {target_root}", file=sys.stderr)
        return 2
    codex_bin = _resolve_codex_bin(args.codex_bin)
    if args.executor_mode == "codex" and not Path(codex_bin).exists():
        print(
            "codex executable was not found. "
            "Set AIDO_CODEX_BIN=/absolute/path/to/codex or run ./osagentd echo for demo mode.",
            file=sys.stderr,
        )
        return 2

    if _session_exists(session):
        if args.replace:
            _kill_session(session)
        else:
            print(
                f"tmux session '{session}' already exists. "
                f"Use --replace or attach manually: tmux attach -t {session}",
                file=sys.stderr,
            )
            return 1

    daemon_cmd = _tmux_shell_command(
        app_dir,
        workdir,
        "python3 -m ai_develop_orchestrator.daemon "
        f"--socket-path {shlex.quote(args.socket_path)} "
        f"--state-file {shlex.quote(args.state_file)} "
        f"--lock-dir {shlex.quote(args.lock_dir)} "
        f"--heartbeat-timeout-ms {args.heartbeat_timeout_ms}",
    )
    monitor_cmd = _tmux_shell_command(
        app_dir,
        workdir,
        "python3 -m ai_develop_orchestrator.monitor "
        f"--socket-path {shlex.quote(args.socket_path)}",
    )
    submitter_cmd = _tmux_shell_command(
        app_dir,
        workdir,
        "python3 -m ai_develop_orchestrator.submitter "
        f"--socket-path {shlex.quote(args.socket_path)} "
        f"--default-capabilities {shlex.quote(args.agent_capabilities)} "
        f"--default-workdir {shlex.quote(target_root)} "
        f"--worker-window {shlex.quote(args.worker_window)}",
    )
    autoscaler_cmd = (
        "python3 -m ai_develop_orchestrator.autoscaler "
        f"--socket-path {shlex.quote(args.socket_path)} "
        f"--session-name {shlex.quote(session)} "
        f"--worker-window {shlex.quote(args.worker_window)} "
        f"--workdir {shlex.quote(workdir)} "
        f"--pythonpath {shlex.quote(str(Path(app_dir) / 'src'))} "
        f"--min-workers {args.min_workers} "
        f"--max-workers {args.max_workers} "
        f"--idle-seconds {args.idle_seconds} "
        f"--agent-capabilities {shlex.quote(args.agent_capabilities)} "
        f"--codex-bin {shlex.quote(codex_bin)} "
        f"--executor-mode {shlex.quote(args.executor_mode)} "
    )
    if args.codex_model:
        autoscaler_cmd += f"--codex-model {shlex.quote(args.codex_model)} "
    if args.bypass_codex_sandbox:
        autoscaler_cmd += "--bypass-codex-sandbox "
    autoscaler_cmd = _tmux_shell_command(app_dir, workdir, autoscaler_cmd)
    main_width = max(20, min(80, args.monitor_width))

    try:
        _run(["tmux", "new-session", "-d", "-s", session, "-n", "control", monitor_cmd])
        _run(["tmux", "set-option", "-t", session, "mouse", "on"])
        _run(["tmux", "set-option", "-t", session, "status", "on"])
        _run(["tmux", "set-option", "-t", session, "status-left", " osagentd "])
        _run(["tmux", "set-option", "-t", session, "status-right", " workers: Ctrl-b n | stop: ./osagentd down "])
        _run(["tmux", "select-pane", "-t", f"{session}:0.0", "-T", "monitor"])
        _run(
            [
                "tmux",
                "new-window",
                "-d",
                "-t",
                session,
                "-n",
                args.worker_window,
                "bash",
                "-lc",
                "printf 'osagentd workers window\\n\\nWorkers will appear here as separate panes.\\nSwitch windows: Ctrl-b n / Ctrl-b p, or click the tmux status bar.\\n\\n'; exec bash",
            ]
        )
        _run(["tmux", "select-pane", "-t", f"{session}:{args.worker_window}.0", "-T", "worker-board"])
        daemon_pane = _split_window(["tmux", "split-window", "-d", "-P", "-F", "#{pane_id}", "-h", "-t", f"{session}:0.0", "-l", f"{100 - main_width}%", daemon_cmd])
        _run(["tmux", "select-pane", "-t", daemon_pane, "-T", "daemon"])
        submitter_pane = _split_window(["tmux", "split-window", "-d", "-P", "-F", "#{pane_id}", "-v", "-t", daemon_pane, submitter_cmd])
        _run(["tmux", "select-pane", "-t", submitter_pane, "-T", "submitter"])
        autoscaler_pane = _split_window(["tmux", "split-window", "-d", "-P", "-F", "#{pane_id}", "-v", "-t", submitter_pane, autoscaler_cmd])
        _run(["tmux", "select-pane", "-t", autoscaler_pane, "-T", "autoscaler"])
        _run(["tmux", "set-window-option", "-t", f"{session}:0", "main-pane-width", f"{main_width}%"])
        _run(["tmux", "select-layout", "-t", f"{session}:0", "main-vertical"])
        _run(["tmux", "select-window", "-t", f"{session}:control"])

        _run(["tmux", "select-pane", "-t", submitter_pane])
    except Exception:
        with contextlib.suppress(Exception):
            _kill_session(session)
        raise

    if args.attach:
        os.execvp("tmux", ["tmux", "attach-session", "-t", session])
    print(f"tmux session created: {session}")
    print(f"attach with: tmux attach -t {session}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
