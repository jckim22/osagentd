from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH
from .results import _group_runs, _latest_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show osagentd run metrics")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--run")
    return parser.parse_args()


def _token_count(log_file: str) -> int:
    if not log_file or not Path(log_file).exists():
        return 0
    text = Path(log_file).read_text(encoding="utf-8", errors="ignore")
    matches = re.findall(r"tokens used\s*\n\s*([0-9,]+)", text)
    if not matches:
        return 0
    return int(matches[-1].replace(",", ""))


def _stage(task: Dict[str, Any]) -> str:
    return str(task.get("task_type") or task.get("stage") or "-")


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    snapshot = request(args.socket_path, {"action": "status"})
    tasks = snapshot.get("tasks", [])
    run_id = args.run or _latest_run(tasks)
    if run_id == "-":
        print("no osagentd runs found")
        return 1
    stages = _group_runs(tasks).get(run_id, {})
    if not stages:
        print(f"run not found: {run_id}")
        return 1

    total_run_ms = sum(int(task.get("run_ms", 0) or 0) for task in stages.values())
    max_stage_ms = max([int(task.get("run_ms", 0) or 0) for task in stages.values()] or [0])
    speedup = (total_run_ms / max_stage_ms) if max_stage_ms else 0.0
    total_tokens = 0

    print(f"osagentd metrics run={run_id}")
    print("")
    print("stage     status     run_ms   wait_ms  tokens   log")
    print("--------- ---------- -------- -------- -------- --------------------------------")
    for name in ["plan", "research", "code", "review", "merge"]:
        task = stages.get(name)
        if not task:
            continue
        result = task.get("result") or {}
        tokens = _token_count(str(result.get("log_file") or ""))
        total_tokens += tokens
        print(
            f"{_stage(task):<9} {str(task.get('status', '-')):<10} "
            f"{int(task.get('run_ms', 0) or 0):<8} "
            f"{int(task.get('wait_ms', 0) or 0):<8} "
            f"{tokens:<8} {result.get('log_file', '-')}"
        )

    print("")
    print(f"serial_runtime_estimate_ms={total_run_ms}")
    print(f"critical_path_runtime_estimate_ms={max_stage_ms}")
    print(f"parallelism_upper_bound={speedup:.2f}x")
    print(f"tokens_used_estimate={total_tokens}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
