from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
from typing import Any, Dict, Optional

from .protocol import decode_message, encode_message, error, ok
from .state import OrchestratorState


DEFAULT_SOCKET_PATH = "/tmp/ai_develop_orchestrator.sock"
DEFAULT_LOCK_DIR = "/tmp/ai_develop_orchestrator_locks"
DEFAULT_STATE_FILE = "/tmp/ai_develop_orchestrator_state.json"


class OrchestratorServer:
    def __init__(self, socket_path: str, lock_dir: str, state_file: str, heartbeat_timeout_ms: int) -> None:
        self.socket_path = socket_path
        self.state = OrchestratorState(lock_dir=lock_dir, state_file=state_file, heartbeat_timeout_ms=heartbeat_timeout_ms)
        self.server: Optional[asyncio.base_events.Server] = None
        self._maintenance_task: Optional[asyncio.Task[Any]] = None
        self._shutdown_event = asyncio.Event()

    async def run(self) -> None:
        parent = os.path.dirname(self.socket_path) or "."
        os.makedirs(parent, exist_ok=True)
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        self.server = await asyncio.start_unix_server(self._handle_client, path=self.socket_path)
        os.chmod(self.socket_path, 0o666)
        self._maintenance_task = asyncio.create_task(self._maintenance_loop())
        logging.info("listening on %s", self.socket_path)
        async with self.server:
            await self._shutdown_event.wait()

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self._maintenance_task:
            self._maintenance_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._maintenance_task
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)

    async def _maintenance_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1.0)
            self.state.expire()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while not reader.at_eof():
                raw = await reader.readline()
                if not raw:
                    break
                request = decode_message(raw)
                response = self._dispatch(request)
                writer.write(encode_message(response))
                await writer.drain()
        except Exception as exc:
            logging.exception("client failure: %s", exc)
            writer.write(encode_message(error(str(exc), code="internal_error")))
            with contextlib.suppress(Exception):
                await writer.drain()
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    def _dispatch(self, request: Dict[str, Any]) -> Dict[str, Any]:
        action = request.get("action")
        try:
            if action == "register":
                agent = self.state.register_agent(
                    name=str(request.get("name", "")),
                    capabilities=request.get("capabilities", []),
                    metadata=request.get("metadata", {}),
                    agent_id=request.get("agent_id"),
                    pid=request.get("pid"),
                )
                return ok(agent_id=agent.agent_id, capabilities=sorted(agent.capabilities))
            if action == "heartbeat":
                self.state.heartbeat(
                    agent_id=str(request["agent_id"]),
                    resources=request.get("resources", []),
                    lease_ttl_ms=int(request.get("lease_ttl_ms", 10000)),
                )
                return ok(agent_id=request["agent_id"])
            if action == "unregister":
                self.state.unregister_agent(str(request["agent_id"]))
                return ok(agent_id=request["agent_id"])
            if action == "submit_task":
                task = self.state.submit_task(
                    task_type=str(request.get("task_type", "generic")),
                    payload=request.get("payload", {}),
                    priority=int(request.get("priority", 0)),
                    required_capabilities=request.get("required_capabilities", []),
                    required_resources=request.get("required_resources", []),
                    lease_ttl_ms=int(request.get("lease_ttl_ms", 10000)),
                    labels=request.get("labels", []),
                    dependencies=request.get("dependencies", []),
                )
                return ok(task_id=task.task_id, status=task.status)
            if action == "poll_task":
                task = self.state.assign_task(
                    agent_id=str(request["agent_id"]),
                    offered_capabilities=request.get("capabilities", []),
                )
                if task is None:
                    return ok(task=None)
                return ok(
                    task={
                        "task_id": task.task_id,
                        "task_type": task.task_type,
                        "payload": task.payload,
                        "priority": task.priority,
                        "required_capabilities": sorted(task.required_capabilities),
                        "required_resources": task.required_resources,
                        "labels": sorted(task.labels),
                    }
                )
            if action == "complete_task":
                task = self.state.complete_task(
                    agent_id=str(request["agent_id"]),
                    task_id=str(request["task_id"]),
                    success=bool(request.get("success", True)),
                    result=request.get("result", {}),
                )
                return ok(task_id=task.task_id, status=task.status)
            if action == "cancel_task":
                task = self.state.cancel_task(str(request["task_id"]))
                return ok(task_id=task.task_id, status=task.status)
            if action == "retry_run":
                result = self.state.retry_run(
                    str(request["run_id"]),
                    include_completed=bool(request.get("include_completed", False)),
                )
                return ok(**result)
            if action == "acquire":
                result = self.state.acquire_resources(
                    agent_id=str(request["agent_id"]),
                    resources=request.get("resources", []),
                    lease_ttl_ms=int(request.get("lease_ttl_ms", 10000)),
                    wait=bool(request.get("wait", False)),
                    timeout_ms=int(request.get("timeout_ms", 0)),
                    task_id=request.get("task_id"),
                )
                return ok(agent_id=request["agent_id"], **result)
            if action == "release":
                released = self.state.release_resources(str(request["agent_id"]), request.get("resources", []))
                return ok(agent_id=request["agent_id"], released=released)
            if action == "status":
                return ok(**self.state.snapshot())
            if action == "shutdown":
                asyncio.create_task(self.shutdown())
                return ok(message="shutdown requested")
            return error(f"unknown action: {action}", code="unknown_action")
        except KeyError as exc:
            return error(f"missing field: {exc.args[0]}", code="bad_request")
        except ValueError as exc:
            return error(str(exc), code="bad_request")
        except RuntimeError as exc:
            return error(str(exc), code="resource_busy")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OS-driven AI development orchestrator daemon")
    parser.add_argument("--socket-path", default=os.environ.get("AIDO_SOCKET_PATH", DEFAULT_SOCKET_PATH))
    parser.add_argument("--lock-dir", default=os.environ.get("AIDO_LOCK_DIR", DEFAULT_LOCK_DIR))
    parser.add_argument("--state-file", default=os.environ.get("AIDO_STATE_FILE", DEFAULT_STATE_FILE))
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=int(os.environ.get("AIDO_HEARTBEAT_TIMEOUT_MS", "30000")))
    parser.add_argument("--log-level", default=os.environ.get("AIDO_LOG_LEVEL", "INFO"))
    return parser.parse_args()


async def _async_main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(asctime)s [%(levelname)s] %(message)s")
    server = OrchestratorServer(
        socket_path=args.socket_path,
        lock_dir=args.lock_dir,
        state_file=args.state_file,
        heartbeat_timeout_ms=args.heartbeat_timeout_ms,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(server.shutdown()))
    await server.run()
    return 0


def main() -> int:
    return asyncio.run(_async_main())


if __name__ == "__main__":
    raise SystemExit(main())
