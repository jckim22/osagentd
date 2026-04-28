from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show recent osagentd run results")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--run")
    parser.add_argument("--list", action="store_true", help="List recent runs instead of printing one result")
    parser.add_argument("--open", action="store_true", help="Print the latest final report path only")
    parser.add_argument("--tail-lines", type=int, default=80)
    return parser.parse_args()


def _labels(task: Dict[str, Any]) -> tuple[str, str]:
    run_id = "-"
    stage = "-"
    for label in task.get("labels", []):
        label = str(label)
        if label.startswith("run:"):
            run_id = label.split(":", 1)[1]
        if label.startswith("stage:"):
            stage = label.split(":", 1)[1]
    return run_id, stage


def _extract_paths(text: str) -> List[str]:
    paths = set(re.findall(r"\]\((/[^)]+)\)", text))
    paths.update(re.findall(r"(/[\w./가-힣 _-]+(?:\.md|\.txt|\.json|\.log|\.html|\.py|\.cpp|\.h|\.ini))", text))
    return sorted(path for path in paths if Path(path).exists())


def _tail(text: str, lines: int) -> str:
    return "\n".join(text.splitlines()[-lines:])


def _latest_run(tasks: List[Dict[str, Any]]) -> str:
    run_ids = [run_id for task in tasks for run_id, _stage in [_labels(task)] if run_id != "-"]
    return run_ids[-1] if run_ids else "-"


def _group_runs(tasks: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    runs: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for task in tasks:
        run_id, stage = _labels(task)
        if run_id == "-":
            continue
        runs.setdefault(run_id, {})[stage] = task
    return runs


def _run_status(stages: Dict[str, Dict[str, Any]]) -> str:
    statuses = {str(task.get("status", "")) for task in stages.values()}
    if "failed" in statuses:
        return "failed"
    if "running" in statuses:
        return "running"
    if "queued" in statuses or "blocked" in statuses:
        return "pending"
    if statuses == {"completed"}:
        return "completed"
    return ",".join(sorted(statuses)) or "-"


def _final_report(stages: Dict[str, Dict[str, Any]]) -> str:
    merge = stages.get("merge") or {}
    result = merge.get("result") or {}
    explicit = str(result.get("final_report") or "")
    if explicit:
        return explicit
    latest = Path(".osagentd/runs/LATEST.md").resolve()
    return str(latest) if latest.exists() else ""


def _print_runs(tasks: List[Dict[str, Any]]) -> None:
    runs = _group_runs(tasks)
    if not runs:
        print("no osagentd runs found")
        return
    print("run      status     stages                         final")
    print("-------- ---------- ------------------------------ --------------------------------")
    for run_id, stages in list(runs.items())[-12:]:
        stage_bits = " ".join(f"{stage}:{str(task.get('status', '-'))[:4]}" for stage, task in stages.items())
        print(f"{run_id:<8} {_run_status(stages):<10} {stage_bits:<30} {_final_report(stages)}")


def _print_run(run_id: str, tasks: List[Dict[str, Any]], tail_lines: int) -> None:
    stages: Dict[str, Dict[str, Any]] = {}
    for task in tasks:
        current_run, stage = _labels(task)
        if current_run == run_id:
            stages[stage] = task

    if not stages:
        print(f"run not found: {run_id}")
        return

    print(f"osagentd result run={run_id}")
    print("")
    for stage in ["plan", "research", "code", "review", "merge"]:
        task = stages.get(stage)
        if not task:
            continue
        result = task.get("result") or {}
        print(
            f"{stage:<8} {task.get('status', '-'):<10} "
            f"task={str(task.get('task_id', ''))[:8]} "
            f"log={result.get('log_file', '-')}"
        )
        if result.get("final_report"):
            print(f"{'':<19} final={result.get('final_report')}")

    texts = []
    for task in stages.values():
        result = task.get("result") or {}
        texts.append(str(result.get("output_tail") or ""))
        if result.get("log_file"):
            texts.append(str(result.get("log_file")))
        if result.get("final_report"):
            texts.append(str(result.get("final_report")))
    paths = sorted({path for text in texts for path in _extract_paths(text)})

    print("")
    print("Produced / referenced files:")
    if paths:
        for path in paths:
            print(f"- {path}")
    else:
        print("- (none detected)")

    merge = stages.get("merge")
    if merge:
        result = merge.get("result") or {}
        output = str(result.get("output_tail") or "")
        if output:
            print("")
            print("Final merge output:")
            print(_tail(output, tail_lines))


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    snapshot = request(args.socket_path, {"action": "status"})
    tasks = snapshot.get("tasks", [])
    if args.list:
        _print_runs(tasks)
        return 0
    run_id = args.run or _latest_run(tasks)
    if run_id == "-":
        print("no osagentd runs found")
        return 1
    if args.open:
        stages = _group_runs(tasks).get(run_id, {})
        path = _final_report(stages)
        if not path:
            print("no final report for run yet")
            return 1
        print(path)
        return 0
    _print_run(run_id, tasks, args.tail_lines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
