from __future__ import annotations

import argparse
import os
from typing import Any, Dict, Iterable, List

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH
from .results import _latest_run


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retry failed osagentd run tasks")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--run")
    parser.add_argument("--all", action="store_true", help="Retry the entire run, including completed tasks")
    return parser.parse_args()


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args()
    snapshot: Dict[str, Any] = request(args.socket_path, {"action": "status"})
    run_id = args.run or _latest_run(snapshot.get("tasks", []))
    if run_id == "-":
        print("no osagentd runs found")
        return 1
    response = request(
        args.socket_path,
        {
            "action": "retry_run",
            "run_id": run_id,
            "include_completed": args.all,
        },
    )
    if not response.get("ok"):
        print(response)
        return 1
    print(f"retry scheduled run={run_id} reset={response.get('reset_count', 0)}")
    for task_id in response.get("reset_task_ids", []):
        print(f"- {task_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
