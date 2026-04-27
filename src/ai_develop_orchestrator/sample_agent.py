from __future__ import annotations

import argparse
import os
import time
from typing import Iterable, List

from .cli import request
from .daemon import DEFAULT_SOCKET_PATH


def _csv(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sample worker agent for AI Develop Orchestrator")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--agent-id")
    parser.add_argument("--name", required=True)
    parser.add_argument("--capabilities", default="code")
    parser.add_argument("--metadata", default="{}")
    parser.add_argument("--heartbeat-ms", type=int, default=3000)
    parser.add_argument("--task-runtime-ms", type=int, default=1000)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    capabilities = _csv(args.capabilities)
    resp = request(
        args.socket_path,
        {
            "action": "register",
            "agent_id": args.agent_id,
            "name": args.name,
            "capabilities": capabilities,
            "metadata": {"sample": True},
            "pid": os.getpid(),
        },
    )
    if not resp.get("ok"):
        raise SystemExit(1)
    agent_id = resp["agent_id"]
    try:
        while True:
            task_resp = request(
                args.socket_path,
                {"action": "poll_task", "agent_id": agent_id, "capabilities": capabilities},
            )
            task = task_resp.get("task")
            if task:
                time.sleep(max(0, args.task_runtime_ms) / 1000.0)
                request(
                    args.socket_path,
                    {
                        "action": "complete_task",
                        "agent_id": agent_id,
                        "task_id": task["task_id"],
                        "success": True,
                        "result": {"handled_by": args.name, "mode": "sample_agent"},
                    },
                )
            request(
                args.socket_path,
                {
                    "action": "heartbeat",
                    "agent_id": agent_id,
                    "resources": [],
                    "lease_ttl_ms": max(1000, args.heartbeat_ms * 2),
                },
            )
            time.sleep(max(250, args.heartbeat_ms) / 1000.0)
    except KeyboardInterrupt:
        request(args.socket_path, {"action": "unregister", "agent_id": agent_id})
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
