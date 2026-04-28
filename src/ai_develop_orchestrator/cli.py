from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from typing import Any, Dict, Iterable, List

from .daemon import DEFAULT_SOCKET_PATH
from .protocol import decode_message, encode_message


def request(socket_path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(socket_path)
        sock.sendall(encode_message(payload))
        raw = b""
        while not raw.endswith(b"\n"):
            chunk = sock.recv(65536)
            if not chunk:
                break
            raw += chunk
    if not raw:
        raise RuntimeError("orchestrator did not reply")
    return decode_message(raw)


def _json_object(raw: str) -> Dict[str, Any]:
    if not raw:
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise argparse.ArgumentTypeError("expected a JSON object")
    return value


def _csv(raw: str) -> List[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AI Develop Orchestrator CLI")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    sub = parser.add_subparsers(dest="command", required=True)

    register = sub.add_parser("register")
    register.add_argument("--agent-id")
    register.add_argument("--name", required=True)
    register.add_argument("--capabilities", default="")
    register.add_argument("--metadata", type=_json_object, default={})
    register.add_argument("--pid", type=int)

    heartbeat = sub.add_parser("heartbeat")
    heartbeat.add_argument("--agent-id", required=True)
    heartbeat.add_argument("--resources", default="")
    heartbeat.add_argument("--lease-ttl-ms", type=int, default=10000)

    unregister = sub.add_parser("unregister")
    unregister.add_argument("--agent-id", required=True)

    submit = sub.add_parser("submit-task")
    submit.add_argument("--task-type", required=True)
    submit.add_argument("--payload", type=_json_object, default={})
    submit.add_argument("--priority", type=int, default=0)
    submit.add_argument("--required-capabilities", default="")
    submit.add_argument("--required-resources", default="")
    submit.add_argument("--lease-ttl-ms", type=int, default=10000)
    submit.add_argument("--labels", default="")

    poll = sub.add_parser("poll-task")
    poll.add_argument("--agent-id", required=True)
    poll.add_argument("--capabilities", default="")

    complete = sub.add_parser("complete-task")
    complete.add_argument("--agent-id", required=True)
    complete.add_argument("--task-id", required=True)
    complete.add_argument("--failure", action="store_true")
    complete.add_argument("--result", type=_json_object, default={})

    cancel = sub.add_parser("cancel-task")
    cancel.add_argument("--task-id", required=True)

    retry = sub.add_parser("retry-run")
    retry.add_argument("--run-id", required=True)
    retry.add_argument("--include-completed", action="store_true")

    acquire = sub.add_parser("acquire")
    acquire.add_argument("--agent-id", required=True)
    acquire.add_argument("--resources", required=True)
    acquire.add_argument("--lease-ttl-ms", type=int, default=10000)
    acquire.add_argument("--wait", action="store_true")
    acquire.add_argument("--timeout-ms", type=int, default=0)
    acquire.add_argument("--task-id")

    release = sub.add_parser("release")
    release.add_argument("--agent-id", required=True)
    release.add_argument("--resources", required=True)

    sub.add_parser("status")
    sub.add_parser("shutdown")
    return parser


def payload_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    if args.command == "register":
        return {
            "action": "register",
            "agent_id": args.agent_id,
            "name": args.name,
            "capabilities": _csv(args.capabilities),
            "metadata": args.metadata,
            "pid": args.pid,
        }
    if args.command == "heartbeat":
        return {
            "action": "heartbeat",
            "agent_id": args.agent_id,
            "resources": _csv(args.resources),
            "lease_ttl_ms": args.lease_ttl_ms,
        }
    if args.command == "unregister":
        return {"action": "unregister", "agent_id": args.agent_id}
    if args.command == "submit-task":
        return {
            "action": "submit_task",
            "task_type": args.task_type,
            "payload": args.payload,
            "priority": args.priority,
            "required_capabilities": _csv(args.required_capabilities),
            "required_resources": _csv(args.required_resources),
            "lease_ttl_ms": args.lease_ttl_ms,
            "labels": _csv(args.labels),
        }
    if args.command == "poll-task":
        return {
            "action": "poll_task",
            "agent_id": args.agent_id,
            "capabilities": _csv(args.capabilities),
        }
    if args.command == "complete-task":
        return {
            "action": "complete_task",
            "agent_id": args.agent_id,
            "task_id": args.task_id,
            "success": not args.failure,
            "result": args.result,
        }
    if args.command == "cancel-task":
        return {"action": "cancel_task", "task_id": args.task_id}
    if args.command == "retry-run":
        return {
            "action": "retry_run",
            "run_id": args.run_id,
            "include_completed": args.include_completed,
        }
    if args.command == "acquire":
        return {
            "action": "acquire",
            "agent_id": args.agent_id,
            "resources": _csv(args.resources),
            "lease_ttl_ms": args.lease_ttl_ms,
            "wait": args.wait,
            "timeout_ms": args.timeout_ms,
            "task_id": args.task_id,
        }
    if args.command == "release":
        return {"action": "release", "agent_id": args.agent_id, "resources": _csv(args.resources)}
    if args.command == "status":
        return {"action": "status"}
    if args.command == "shutdown":
        return {"action": "shutdown"}
    raise ValueError(f"unsupported command: {args.command}")


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    response = request(args.socket_path, payload_from_args(args))
    json.dump(response, sys.stdout, indent=2, ensure_ascii=True)
    sys.stdout.write("\n")
    return 0 if response.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
