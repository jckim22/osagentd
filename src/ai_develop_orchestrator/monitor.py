from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from datetime import datetime
from textwrap import shorten
from typing import Any, Dict, Iterable, List, Tuple

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live status monitor for osagentd")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--interval-ms", type=int, default=2000)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _render_table(items: List[Dict[str, Any]], columns: List[str]) -> List[str]:
    if not items:
        return ["  (empty)"]
    widths = {col: len(col) for col in columns}
    rows: List[Dict[str, str]] = []
    for item in items:
        row = {col: str(item.get(col, "")) for col in columns}
        rows.append(row)
        for col, value in row.items():
            widths[col] = max(widths[col], len(value))
    header = "  " + " | ".join(col.ljust(widths[col]) for col in columns)
    sep = "  " + "-+-".join("-" * widths[col] for col in columns)
    body = [
        "  " + " | ".join(row[col].ljust(widths[col]) for col in columns)
        for row in rows
    ]
    return [header, sep, *body]


def _rule(char: str = "=") -> str:
    width = shutil.get_terminal_size((100, 40)).columns
    return char * max(40, min(width - 1, 120))


def _task_index(snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {str(task.get("task_id")): task for task in snapshot.get("tasks", [])}


def _agent_rows(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    tasks_by_id = _task_index(snapshot)
    for agent in snapshot.get("agents", []):
        task_id = str(agent.get("current_task_id") or "")
        task = tasks_by_id.get(task_id, {})
        doing = task.get("prompt_preview") or "idle"
        rows.append(
            {
                "name": shorten(str(agent.get("name", "")), width=12, placeholder=".."),
                "role": str(agent.get("name", "")).split("-", 1)[0],
                "agent": str(agent.get("agent_id", ""))[:8],
                "task": str(agent.get("current_task_id") or "-")[:8],
                "doing": shorten(str(doing), width=44, placeholder=".."),
                "resources": shorten(",".join(agent.get("resources", [])) or "-", width=18, placeholder=".."),
                "seen": agent.get("last_seen_ms_ago", 0),
            }
        )
    return rows


def _task_rows(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for task in snapshot.get("tasks", []):
        rows.append(
            {
                "task": str(task.get("task_id", ""))[:8],
                "type": shorten(str(task.get("task_type", "")), width=12, placeholder=".."),
                "status": shorten(str(task.get("status", "")), width=10, placeholder=".."),
                "prio": task.get("priority", 0),
                "agent": str(task.get("assigned_agent_id") or "-")[:8],
                "wait": task.get("wait_ms", 0),
                "run": task.get("run_ms", 0),
                "prompt": shorten(str(task.get("prompt_preview") or "-"), width=42, placeholder=".."),
                "resources": shorten(",".join(task.get("required_resources", [])) or "-", width=20, placeholder=".."),
            }
        )
    return rows


def _labels(task: Dict[str, Any]) -> Tuple[str, str]:
    run_id = "-"
    stage = "-"
    for label in task.get("labels", []):
        if str(label).startswith("run:"):
            run_id = str(label).split(":", 1)[1]
        if str(label).startswith("stage:"):
            stage = str(label).split(":", 1)[1]
    return run_id, stage


def _run_rows(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for task in snapshot.get("tasks", []):
        run_id, stage = _labels(task)
        if run_id == "-":
            continue
        grouped.setdefault(run_id, {})[stage] = task

    rows: List[Dict[str, Any]] = []
    for run_id, stages in sorted(grouped.items()):
        run_ms_values = [int(task.get("run_ms", 0) or 0) for task in stages.values()]
        serial_ms = sum(run_ms_values)
        parallel_ms = max(run_ms_values) if run_ms_values else 0
        efficiency = f"{serial_ms / parallel_ms:.1f}x" if parallel_ms > 0 and len(run_ms_values) > 1 else "-"
        active = sum(1 for task in stages.values() if task.get("status") == "running")
        queued = sum(1 for task in stages.values() if task.get("status") == "queued")
        blocked = sum(1 for task in stages.values() if task.get("status") == "blocked")
        rows.append(
            {
                "run": run_id,
                "plan": _stage_mark(stages.get("plan")),
                "research": _stage_mark(stages.get("research")),
                "code": _stage_mark(stages.get("code")),
                "review": _stage_mark(stages.get("review")),
                "merge": _stage_mark(stages.get("merge")),
                "active": active,
                "queue": queued,
                "block": blocked,
                "eff": efficiency,
            }
        )
    return rows[-8:]


def _stage_mark(task: Dict[str, Any] | None) -> str:
    if not task:
        return "-"
    status = str(task.get("status", ""))
    if status == "completed":
        return "done"
    if status == "running":
        return "run"
    if status == "queued":
        return "wait"
    if status == "blocked":
        return "hold"
    if status == "failed":
        return "fail"
    return status[:4] or "-"


def _lease_rows(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for lease in snapshot.get("leases", []):
        rows.append(
            {
                "resource": shorten(str(lease.get("resource_id", "")), width=24, placeholder=".."),
                "owner": str(lease.get("owner_agent_id", ""))[:8],
                "task": str(lease.get("task_id") or "-")[:8],
                "ttl": lease.get("lease_ms_remaining", 0),
            }
        )
    return rows


def _recent_results(snapshot: Dict[str, Any]) -> List[str]:
    done = [
        item for item in snapshot.get("tasks", [])
        if item.get("status") in {"completed", "failed", "cancelled"}
    ]
    done = done[-3:]
    if not done:
        return ["  (none)"]
    lines: List[str] = []
    for item in done:
        result = item.get("result") or {}
        summary = shorten(str(result.get("summary", "-")), width=54, placeholder="..")
        lines.append(
            f"  {str(item.get('task_id', ''))[:8]}  {str(item.get('status', '')):<9}  {summary}"
        )
    return lines


def _render_text(snapshot: Dict[str, Any]) -> str:
    lines: List[str] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(_rule("="))
    lines.append(f"osagentd  |  OS-native control plane  |  {now}")
    lines.append(_rule("="))
    lines.append("")
    metrics = snapshot.get("metrics", {})
    task_list = snapshot.get("tasks", [])
    running = metrics.get("running_tasks", sum(1 for item in task_list if item.get("status") == "running"))
    queued = metrics.get("queued_tasks", sum(1 for item in task_list if item.get("status") == "queued"))
    failed = metrics.get("failed_tasks", sum(1 for item in task_list if item.get("status") == "failed"))
    summary = (
        f"agents={metrics.get('agent_count', len(snapshot.get('agents', [])))}  "
        f"active={metrics.get('active_agents', 0)}  idle={metrics.get('idle_agents', 0)}  "
        f"queued={queued}  running={running}  failed={failed}  "
        f"avg_run={metrics.get('avg_runtime_ms', 0)}ms  "
        f"leases={len(snapshot.get('leases', []))}"
    )
    lines.append(summary)
    lines.append("input=submitter pane  workers=Ctrl-b n  control=Ctrl-b p  stop=./osagentd down")
    lines.append("")
    lines.append(_rule("-"))
    lines.append("RUNS: role split + parallelism")
    lines.append(_rule("-"))
    lines.extend(_render_table(_run_rows(snapshot), ["run", "plan", "research", "code", "review", "merge", "active", "queue", "block", "eff"]))
    lines.append("")
    lines.append(_rule("-"))
    lines.append("RECENT RESULTS")
    lines.append(_rule("-"))
    lines.extend(_recent_results(snapshot))
    lines.append("")
    lines.append(_rule("-"))
    lines.append("AGENTS")
    lines.append(_rule("-"))
    lines.extend(_render_table(_agent_rows(snapshot), ["name", "role", "task", "doing", "resources", "seen"]))
    lines.append("")
    lines.append(_rule("-"))
    lines.append("TASKS")
    lines.append(_rule("-"))
    lines.extend(_render_table(_task_rows(snapshot), ["task", "type", "status", "agent", "run", "prompt", "resources"]))
    lines.append("")
    lines.append(_rule("-"))
    lines.append("LEASES")
    lines.append(_rule("-"))
    lines.extend(_render_table(_lease_rows(snapshot), ["resource", "owner", "task", "ttl"]))
    lines.append("")
    lines.append(_rule("-"))
    lines.append("PENDING")
    lines.append(_rule("-"))
    pending = snapshot.get("pending_requests", [])
    if not pending:
        lines.append("  (empty)")
    else:
        for item in pending:
            lines.append(
                f"  {item.get('request_id')} agent={item.get('agent_id')} resources={','.join(item.get('resources', []))} expires_ms={item.get('ms_until_expiry', 0)}"
            )
    return "\n".join(lines)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    interval_sec = max(0.2, args.interval_ms / 1000.0)
    while True:
        try:
            response = request(args.socket_path, {"action": "status"})
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            output = f"osagentd monitor\n\nerror: {exc}"
        else:
            if args.json:
                output = json.dumps(response, indent=2, ensure_ascii=True)
            else:
                output = _render_text(response)
        print("\033[2J\033[H", end="")
        print(output, flush=True)
        try:
            time.sleep(interval_sec)
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
