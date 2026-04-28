from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
from textwrap import fill, shorten
from typing import Any, Dict, Iterable, List

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH


def _csv(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Codex-backed worker for osagentd")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--agent-id")
    parser.add_argument("--name", required=True)
    parser.add_argument("--capabilities", default="code,python")
    parser.add_argument("--heartbeat-ms", type=int, default=3000)
    parser.add_argument("--workdir", default=os.getcwd())
    parser.add_argument("--codex-bin", default=os.environ.get("AIDO_CODEX_BIN", "codex"))
    parser.add_argument("--codex-model", default=os.environ.get("AIDO_CODEX_MODEL", ""))
    parser.add_argument("--executor-mode", choices=["codex", "echo"], default=os.environ.get("AIDO_EXECUTOR_MODE", "codex"))
    parser.add_argument("--sandbox", default=os.environ.get("AIDO_CODEX_SANDBOX", "workspace-write"))
    parser.add_argument("--approval", default=os.environ.get("AIDO_CODEX_APPROVAL", "never"))
    parser.add_argument("--danger-full-auto", action="store_true")
    parser.add_argument("--bypass-codex-sandbox", action="store_true")
    return parser.parse_args()


def build_prompt(task: Dict[str, Any]) -> str:
    payload = task.get("payload", {}) if isinstance(task.get("payload"), dict) else {}
    prompt = payload.get("prompt")
    if isinstance(prompt, str) and prompt.strip():
        return prompt.strip()
    goal = payload.get("goal")
    resources = ", ".join(task.get("required_resources", []))
    lines = [
        f"Task type: {task.get('task_type', 'generic')}",
        f"Priority: {task.get('priority', 0)}",
    ]
    if goal:
        lines.append(f"Goal: {goal}")
    if resources:
        lines.append(f"Resources: {resources}")
    lines.append("Please complete this task in the current workspace and summarize the result.")
    return "\n".join(lines)


def _should_hide_noise(line: str) -> bool:
    noise_tokens = [
        "WARN codex_core_plugins::manifest",
        "WARN codex_core::shell_snapshot",
        "WARN codex_core::file_watcher",
    ]
    return any(token in line for token in noise_tokens)


def _run_id(task: Dict[str, Any]) -> str:
    for label in task.get("labels", []):
        label = str(label)
        if label.startswith("run:"):
            return label.split(":", 1)[1]
    payload = task.get("payload", {}) if isinstance(task.get("payload"), dict) else {}
    return str(payload.get("run_id") or "ungrouped")


def _write_final_report(args: argparse.Namespace, task: Dict[str, Any], content: str) -> str:
    run_id = _run_id(task)
    run_dir = Path(args.workdir) / ".osagentd" / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "FINAL.md"
    report_path.write_text(content.strip() + "\n", encoding="utf-8")
    latest_path = Path(args.workdir) / ".osagentd" / "runs" / "LATEST.md"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(content.strip() + "\n", encoding="utf-8")
    return str(report_path)


def run_task(args: argparse.Namespace, task: Dict[str, Any], log_dir: Path) -> Dict[str, Any]:
    prompt = build_prompt(task)
    task_id = task["task_id"]
    task_type = str(task.get("task_type", "generic"))
    payload = task.get("payload", {}) if isinstance(task.get("payload"), dict) else {}
    target_workdir = str(payload.get("workdir") or args.workdir)
    original_prompt = str(payload.get("original_prompt") or prompt)
    task_brief = str(payload.get("task_brief") or task_type)
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path = log_dir / f"{task_id}.log"
    if args.executor_mode == "echo":
        print("=" * 80, flush=True)
        print(f"[{args.name}] ASSIGNED task={task_id[:8]} type={task_type}", flush=True)
        print(f"[{args.name}] workdir={target_workdir}", flush=True)
        print(f"[{args.name}] task-brief={shorten(task_brief, width=68, placeholder='..')}", flush=True)
        print(f"[{args.name}] original-request={shorten(original_prompt, width=60, placeholder='..')}", flush=True)
        print(f"[{args.name}] role-prompt", flush=True)
        print(fill(prompt, width=76), flush=True)
        print("=" * 80, flush=True)
        out_path.write_text(f"[echo mode]\nworkdir={target_workdir}\n\n{prompt}\n", encoding="utf-8")
        time.sleep(1.0)
        final_report = ""
        if task_type == "merge":
            final_report = _write_final_report(
                args,
                task,
                f"# osagentd final report\n\nEcho-mode merge completed for: {original_prompt}",
            )
        print("=" * 80, flush=True)
        print(f"[{args.name}] COMPLETED task={task_id[:8]} exit=0", flush=True)
        if final_report:
            print(f"[{args.name}] final-report={final_report}", flush=True)
        print(f"[{args.name}] waiting for next task...", flush=True)
        print("=" * 80, flush=True)
        return {
            "summary": f"{task_type} echo-mode completed",
            "log_file": str(out_path),
            "final_report": final_report,
            "output_tail": f"{args.name} completed {task_type} for: {original_prompt}",
        }

    cmd = [args.codex_bin, "--no-alt-screen"]
    if args.bypass_codex_sandbox:
        cmd.extend(["exec", "--dangerously-bypass-approvals-and-sandbox", prompt, "-C", target_workdir])
    else:
        cmd.extend(["-a", args.approval, "exec", prompt, "-C", target_workdir, "-s", args.sandbox])
    if args.codex_model:
        cmd.extend(["-m", args.codex_model])
    if args.danger_full_auto and not args.bypass_codex_sandbox:
        cmd.append("--dangerously-bypass-approvals-and-sandbox")

    with out_path.open("w", encoding="utf-8") as handle:
        banner = [
            "=" * 80,
            f"[{args.name}] ASSIGNED task={task_id[:8]} type={task_type}",
            f"[{args.name}] workdir      {target_workdir}",
            f"[{args.name}] task-brief   {shorten(task_brief, width=64, placeholder='..')}",
            f"[{args.name}] original     {shorten(original_prompt, width=64, placeholder='..')}",
            f"[{args.name}] log          {out_path}",
            f"[{args.name}] cmd          {' '.join(shlex.quote(part) for part in cmd)}",
            "=" * 80,
            "",
        ]
        banner_text = "\n".join(banner)
        print(banner_text, flush=True)
        handle.write(banner_text)
        handle.flush()
        output_tail: deque[str] = deque(maxlen=80)
        output_capture: deque[str] = deque(maxlen=2000)
        proc = subprocess.Popen(
            cmd,
            cwd=target_workdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert proc.stdout is not None
        for line in proc.stdout:
            handle.write(line)
            handle.flush()
            if not _should_hide_noise(line):
                output_tail.append(line.rstrip())
                output_capture.append(line.rstrip())
                sys.stdout.write(line)
                sys.stdout.flush()
        exit_code = proc.wait()

    final_report = ""
    if task_type == "merge" and output_capture:
        final_report = _write_final_report(args, task, "\n".join(output_capture))
    print("", flush=True)
    print("=" * 80, flush=True)
    print(f"[{args.name}] COMPLETED task={task_id[:8]} exit={exit_code}", flush=True)
    if final_report:
        print(f"[{args.name}] final-report={final_report}", flush=True)
    print(f"[{args.name}] waiting for next task...", flush=True)
    print("=" * 80, flush=True)
    return {
        "summary": "codex completed" if exit_code == 0 else "codex failed",
        "exit_code": exit_code,
        "log_file": str(out_path),
        "workdir": target_workdir,
        "final_report": final_report,
        "output_tail": "\n".join(output_tail),
    }


def heartbeat_loop(stop_event: threading.Event, args: argparse.Namespace, agent_id: str) -> None:
    while not stop_event.is_set():
        try:
            request(
                args.socket_path,
                {
                    "action": "heartbeat",
                    "agent_id": agent_id,
                    "resources": [],
                    "lease_ttl_ms": max(1000, args.heartbeat_ms * 2),
                },
            )
        except Exception:
            pass
        stop_event.wait(max(0.25, args.heartbeat_ms / 1000.0))


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    capabilities = _csv(args.capabilities)
    reg = request(
        args.socket_path,
        {
            "action": "register",
            "agent_id": args.agent_id,
            "name": args.name,
            "capabilities": capabilities,
            "metadata": {
                "worker_type": "codex",
                "workdir": args.workdir,
                "executor_mode": args.executor_mode,
            },
            "pid": os.getpid(),
        },
    )
    if not reg.get("ok"):
        return 1
    agent_id = reg["agent_id"]
    print("=" * 80, flush=True)
    print(f"[{args.name}] online capabilities={','.join(capabilities)} mode={args.executor_mode}", flush=True)
    print(f"[{args.name}] waiting for tasks from osagentd...", flush=True)
    print("=" * 80, flush=True)
    log_dir = Path(args.workdir) / ".osagentd" / "worker_logs"
    stop_event = threading.Event()
    hb = threading.Thread(target=heartbeat_loop, args=(stop_event, args, agent_id), daemon=True)
    hb.start()
    try:
        while True:
            task_resp = request(
                args.socket_path,
                {
                    "action": "poll_task",
                    "agent_id": agent_id,
                    "capabilities": capabilities,
                },
            )
            task = task_resp.get("task")
            if not task:
                time.sleep(1.0)
                continue
            print(f"[{args.name}] picked task {str(task.get('task_id', ''))[:8]} ({task.get('task_type')})", flush=True)
            result = run_task(args, task, log_dir)
            success = result.get("exit_code", 0) == 0
            request(
                args.socket_path,
                {
                    "action": "complete_task",
                    "agent_id": agent_id,
                    "task_id": task["task_id"],
                    "success": success,
                    "result": result,
                },
            )
    except KeyboardInterrupt:
        return 0
    finally:
        stop_event.set()
        request(args.socket_path, {"action": "unregister", "agent_id": agent_id})


if __name__ == "__main__":
    raise SystemExit(main())
