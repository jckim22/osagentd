from __future__ import annotations

import contextlib
import errno
import fcntl
import json
import os
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional

from .models import AcquireRequest, AgentRecord, ResourceLease, TaskRecord


class OrchestratorState:
    def __init__(self, lock_dir: str, state_file: str, heartbeat_timeout_ms: int) -> None:
        self.lock_dir = Path(lock_dir)
        self.state_file = Path(state_file)
        self.heartbeat_timeout_ms = heartbeat_timeout_ms
        self.agents: Dict[str, AgentRecord] = {}
        self.leases: Dict[str, ResourceLease] = {}
        self.pending: Deque[AcquireRequest] = deque()
        self.tasks: Dict[str, TaskRecord] = {}
        self.task_queue: Deque[str] = deque()
        self.lock_dir.mkdir(parents=True, exist_ok=True)

    def now(self) -> float:
        return time.monotonic()

    def register_agent(
        self,
        *,
        name: str,
        capabilities: Iterable[str],
        metadata: Optional[Dict[str, Any]] = None,
        agent_id: Optional[str] = None,
        pid: Optional[int] = None,
    ) -> AgentRecord:
        record = AgentRecord(
            agent_id=agent_id or str(uuid.uuid4()),
            name=name or agent_id or "agent",
            capabilities={cap for cap in capabilities if cap},
            metadata=metadata or {},
            last_seen_monotonic=self.now(),
            pid=pid,
        )
        self.agents[record.agent_id] = record
        self._persist_state()
        return record

    def heartbeat(self, agent_id: str, resources: Iterable[str], lease_ttl_ms: int) -> None:
        agent = self._require_agent(agent_id)
        agent.last_seen_monotonic = self.now()
        self.extend_resources(agent_id, resources, lease_ttl_ms)
        self._persist_state()

    def unregister_agent(self, agent_id: str) -> None:
        if agent_id not in self.agents:
            return
        current_task_id = self.agents[agent_id].current_task_id
        self.release_resources(agent_id, self.resources_owned_by(agent_id))
        if current_task_id and current_task_id in self.tasks:
            task = self.tasks[current_task_id]
            task.status = "queued"
            task.assigned_agent_id = None
            self._enqueue_task(task.task_id)
        del self.agents[agent_id]
        self.pending = deque(req for req in self.pending if req.agent_id != agent_id)
        self._persist_state()

    def submit_task(
        self,
        *,
        task_type: str,
        payload: Dict[str, Any],
        priority: int,
        required_capabilities: Iterable[str],
        required_resources: Iterable[str],
        lease_ttl_ms: int,
        labels: Iterable[str],
        dependencies: Iterable[str] = (),
    ) -> TaskRecord:
        task = TaskRecord(
            task_id=str(uuid.uuid4()),
            task_type=task_type,
            payload=payload,
            priority=priority,
            required_capabilities={cap for cap in required_capabilities if cap},
            required_resources=self._normalize_resources(required_resources),
            lease_ttl_ms=max(1000, lease_ttl_ms),
            created_monotonic=self.now(),
            labels={label for label in labels if label},
            dependencies=[dep for dep in dependencies if dep],
        )
        self.tasks[task.task_id] = task
        self._enqueue_task(task.task_id)
        self._persist_state()
        return task

    def assign_task(self, agent_id: str, offered_capabilities: Iterable[str]) -> Optional[TaskRecord]:
        agent = self._require_agent(agent_id)
        if agent.current_task_id:
            return self.tasks.get(agent.current_task_id)
        advertised = set(offered_capabilities) or set(agent.capabilities)
        for task_id in list(self.task_queue):
            task = self.tasks.get(task_id)
            if not task or task.status != "queued":
                continue
            if not self._dependencies_satisfied(task):
                continue
            if task.required_capabilities and not task.required_capabilities.issubset(advertised):
                continue
            if not self._can_grant(task.required_resources):
                continue
            self._inject_dependency_context(task)
            self.task_queue.remove(task_id)
            if task.required_resources:
                self._grant_resources(
                    agent_id=agent_id,
                    resources=task.required_resources,
                    lease_ttl_ms=task.lease_ttl_ms,
                    task_id=task.task_id,
                )
            task.status = "running"
            task.assigned_agent_id = agent_id
            task.started_monotonic = self.now()
            agent.current_task_id = task.task_id
            agent.last_seen_monotonic = self.now()
            self._persist_state()
            return task
        return None

    def complete_task(self, agent_id: str, task_id: str, success: bool, result: Dict[str, Any]) -> TaskRecord:
        agent = self._require_agent(agent_id)
        task = self._require_task(task_id)
        if task.assigned_agent_id != agent_id:
            raise ValueError(f"task {task_id} is not assigned to agent {agent_id}")
        task.status = "completed" if success else "failed"
        task.result = result
        task.completed_monotonic = self.now()
        self.release_resources(agent_id, task.required_resources)
        agent.current_task_id = None
        self._persist_state()
        return task

    def acquire_resources(
        self,
        *,
        agent_id: str,
        resources: Iterable[str],
        lease_ttl_ms: int,
        wait: bool,
        timeout_ms: int,
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._require_agent(agent_id)
        normalized = self._normalize_resources(resources)
        self.expire()
        if not normalized:
            return {"granted": [], "pending": False}
        if self._can_grant(normalized):
            granted = self._grant_resources(
                agent_id=agent_id,
                resources=normalized,
                lease_ttl_ms=lease_ttl_ms,
                task_id=task_id,
            )
            self._persist_state()
            return {"granted": granted, "pending": False}
        if not wait:
            return {
                "granted": [],
                "pending": False,
                "blocked_by": {
                    resource_id: self.leases[resource_id].owner_agent_id
                    for resource_id in normalized
                    if resource_id in self.leases
                },
            }
        request = AcquireRequest(
            request_id=str(uuid.uuid4()),
            agent_id=agent_id,
            resources=normalized,
            lease_ttl_ms=max(1000, lease_ttl_ms),
            expires_monotonic=self.now() + max(0, timeout_ms) / 1000.0,
            task_id=task_id,
        )
        self.pending.append(request)
        self._persist_state()
        return {"granted": [], "pending": True, "request_id": request.request_id}

    def release_resources(self, agent_id: str, resources: Iterable[str]) -> List[str]:
        released: List[str] = []
        for resource_id in self._normalize_resources(resources):
            lease = self.leases.get(resource_id)
            if not lease or lease.owner_agent_id != agent_id:
                continue
            self._unlock_lease(lease)
            del self.leases[resource_id]
            released.append(resource_id)
        if released:
            self._service_pending()
            self._persist_state()
        return released

    def extend_resources(self, agent_id: str, resources: Iterable[str], lease_ttl_ms: int) -> List[str]:
        deadline = self.now() + max(1000, lease_ttl_ms) / 1000.0
        extended: List[str] = []
        for resource_id in self._normalize_resources(resources):
            lease = self.leases.get(resource_id)
            if lease and lease.owner_agent_id == agent_id:
                lease.lease_until_monotonic = deadline
                extended.append(resource_id)
        return extended

    def cancel_task(self, task_id: str) -> TaskRecord:
        task = self._require_task(task_id)
        if task.status == "running" and task.assigned_agent_id:
            self.release_resources(task.assigned_agent_id, task.required_resources)
            agent = self.agents.get(task.assigned_agent_id)
            if agent:
                agent.current_task_id = None
        task.status = "cancelled"
        task.completed_monotonic = self.now()
        self.task_queue = deque(item for item in self.task_queue if item != task_id)
        self._persist_state()
        return task

    def retry_run(self, run_id: str, *, include_completed: bool = False) -> Dict[str, Any]:
        run_tasks = [task for task in self.tasks.values() if f"run:{run_id}" in task.labels]
        if not run_tasks:
            raise ValueError(f"run not found: {run_id}")

        retry_ids = {
            task.task_id for task in run_tasks
            if include_completed or task.status in {"failed", "cancelled"}
        }
        changed = True
        while changed:
            changed = False
            for task in run_tasks:
                if task.task_id in retry_ids:
                    continue
                if any(dep_id in retry_ids for dep_id in task.dependencies):
                    retry_ids.add(task.task_id)
                    changed = True

        reset: List[str] = []
        for task in run_tasks:
            if task.task_id not in retry_ids:
                continue
            if task.assigned_agent_id:
                agent = self.agents.get(task.assigned_agent_id)
                if agent and agent.current_task_id == task.task_id:
                    agent.current_task_id = None
                self.release_resources(task.assigned_agent_id, task.required_resources)
            task.status = "queued"
            task.assigned_agent_id = None
            task.started_monotonic = None
            task.completed_monotonic = None
            task.result = None
            if isinstance(task.payload, dict):
                task.payload.pop("dependency_context_injected", None)
                if task.payload.get("base_prompt"):
                    task.payload["prompt"] = task.payload.pop("base_prompt")
            self._enqueue_task(task.task_id)
            reset.append(task.task_id)

        self._persist_state()
        return {"run_id": run_id, "reset_task_ids": reset, "reset_count": len(reset)}

    def resources_owned_by(self, agent_id: str) -> List[str]:
        return sorted(resource_id for resource_id, lease in self.leases.items() if lease.owner_agent_id == agent_id)

    def expire(self) -> None:
        self._expire_stale_agents()
        self._expire_leases()
        self._service_pending()

    def snapshot(self) -> Dict[str, Any]:
        self.expire()
        return self._build_snapshot()

    def _persist_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(self._build_snapshot(), indent=2, ensure_ascii=True), encoding="utf-8")

    def _build_snapshot(self) -> Dict[str, Any]:
        now = self.now()
        task_values = list(self.tasks.values())
        completed = [task for task in task_values if task.completed_monotonic is not None]
        running = [task for task in task_values if task.status == "running"]
        runnable = [task for task in task_values if task.status == "queued" and self._dependencies_satisfied(task)]
        blocked = [task for task in task_values if task.status == "queued" and not self._dependencies_satisfied(task)]
        total_runtime = sum(
            (task.completed_monotonic or now) - (task.started_monotonic or now)
            for task in completed
        )
        avg_runtime_ms = int((total_runtime / len(completed)) * 1000) if completed else 0
        active_worker_count = sum(1 for agent in self.agents.values() if agent.current_task_id)
        worker_count = len(self.agents)
        return {
            "metrics": {
                "agent_count": worker_count,
                "active_agents": active_worker_count,
                "idle_agents": max(0, worker_count - active_worker_count),
                "queued_tasks": len(runnable),
                "blocked_tasks": len(blocked),
                "running_tasks": len(running),
                "completed_tasks": sum(1 for task in task_values if task.status == "completed"),
                "failed_tasks": sum(1 for task in task_values if task.status == "failed"),
                "avg_runtime_ms": avg_runtime_ms,
            },
            "agents": [
                {
                    "agent_id": agent.agent_id,
                    "name": agent.name,
                    "capabilities": sorted(agent.capabilities),
                    "metadata": agent.metadata,
                    "pid": agent.pid,
                    "current_task_id": agent.current_task_id,
                    "resources": self.resources_owned_by(agent.agent_id),
                    "last_seen_ms_ago": int((now - agent.last_seen_monotonic) * 1000),
                }
                for agent in sorted(self.agents.values(), key=lambda item: item.name)
            ],
            "leases": [
                {
                    "resource_id": lease.resource_id,
                    "owner_agent_id": lease.owner_agent_id,
                    "task_id": lease.task_id,
                    "lease_ms_remaining": max(0, int((lease.lease_until_monotonic - now) * 1000)),
                }
                for lease in sorted(self.leases.values(), key=lambda item: item.resource_id)
            ],
            "pending_requests": [
                {
                    "request_id": req.request_id,
                    "agent_id": req.agent_id,
                    "resources": req.resources,
                    "task_id": req.task_id,
                    "ms_until_expiry": max(0, int((req.expires_monotonic - now) * 1000)),
                }
                for req in self.pending
            ],
            "tasks": [
                {
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "status": self._display_status(task),
                    "priority": task.priority,
                    "assigned_agent_id": task.assigned_agent_id,
                    "wait_ms": int(((task.started_monotonic or now) - task.created_monotonic) * 1000),
                    "run_ms": int(((task.completed_monotonic or now) - task.started_monotonic) * 1000) if task.started_monotonic else 0,
                    "required_capabilities": sorted(task.required_capabilities),
                    "required_resources": task.required_resources,
                    "labels": sorted(task.labels),
                    "dependencies": task.dependencies,
                    "blocked_by": self._blocked_by(task),
                    "workdir": str(task.payload.get("workdir", "")) if isinstance(task.payload, dict) else "",
                    "stage": str(task.payload.get("stage", "")) if isinstance(task.payload, dict) else "",
                    "prompt_preview": self._prompt_preview(task.payload),
                    "result": task.result,
                }
                for task in sorted(self.tasks.values(), key=lambda item: item.created_monotonic)
            ],
        }

    def _prompt_preview(self, payload: Dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        prompt = str(payload.get("task_brief") or payload.get("original_prompt") or payload.get("prompt") or "")
        compact = " ".join(prompt.split())
        return compact[:160]

    def _dependencies_satisfied(self, task: TaskRecord) -> bool:
        return all(
            dep_id in self.tasks and self.tasks[dep_id].status == "completed"
            for dep_id in task.dependencies
        )

    def _blocked_by(self, task: TaskRecord) -> List[str]:
        return [
            dep_id for dep_id in task.dependencies
            if dep_id not in self.tasks or self.tasks[dep_id].status != "completed"
        ]

    def _display_status(self, task: TaskRecord) -> str:
        if task.status == "queued" and not self._dependencies_satisfied(task):
            return "blocked"
        return task.status

    def _inject_dependency_context(self, task: TaskRecord) -> None:
        if not task.dependencies or not isinstance(task.payload, dict):
            return
        if task.payload.get("dependency_context_injected"):
            return
        base_prompt = str(task.payload.get("prompt", ""))
        context = self._dependency_context(task)
        if not context:
            return
        task.payload["base_prompt"] = base_prompt
        task.payload["prompt"] = "\n\n".join(
            [
                base_prompt,
                "UPSTREAM WORKER OUTPUTS PROVIDED BY OSAGENTD:",
                context,
                "Use the upstream outputs above as inputs. Do not blindly repeat them; integrate them into your role-specific work.",
            ]
        )
        task.payload["dependency_context_injected"] = True

    def _dependency_context(self, task: TaskRecord) -> str:
        sections: List[str] = []
        for dep_id in task.dependencies:
            dep = self.tasks.get(dep_id)
            if not dep:
                continue
            result = dep.result or {}
            stage = dep.task_type
            summary = str(result.get("summary") or "-")
            output_tail = str(result.get("output_tail") or "").strip()
            log_file = str(result.get("log_file") or "")
            parts = [
                f"[{stage} {dep.task_id[:8]}]",
                f"status: {dep.status}",
                f"summary: {summary}",
            ]
            if output_tail:
                parts.extend(["output_tail:", output_tail])
            if log_file:
                parts.append(f"log_file: {log_file}")
            sections.append("\n".join(parts))
        return "\n\n".join(sections)

    def _normalize_resources(self, resources: Iterable[str]) -> List[str]:
        normalized: List[str] = []
        for item in resources:
            token = "".join(ch if ch.isalnum() or ch in ("_", "-", ".", "/") else "_" for ch in str(item).strip())
            if token and token not in normalized:
                normalized.append(token)
        normalized.sort()
        return normalized

    def _enqueue_task(self, task_id: str) -> None:
        if task_id not in self.task_queue:
            self.task_queue.append(task_id)
        self.task_queue = deque(
            sorted(
                self.task_queue,
                key=lambda current: (-self.tasks[current].priority, self.tasks[current].created_monotonic),
            )
        )

    def _can_grant(self, resources: Iterable[str]) -> bool:
        return all(resource_id not in self.leases for resource_id in resources)

    def _grant_resources(
        self,
        *,
        agent_id: str,
        resources: List[str],
        lease_ttl_ms: int,
        task_id: Optional[str] = None,
    ) -> List[str]:
        created: List[ResourceLease] = []
        deadline = self.now() + max(1000, lease_ttl_ms) / 1000.0
        try:
            for resource_id in resources:
                lock_path = self.lock_dir / f"{resource_id.replace('/', '__')}.lock"
                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    os.close(fd)
                    if exc.errno in (errno.EAGAIN, errno.EACCES):
                        raise RuntimeError(f"resource {resource_id} is already locked") from exc
                    raise
                created.append(
                    ResourceLease(
                        resource_id=resource_id,
                        owner_agent_id=agent_id,
                        lease_until_monotonic=deadline,
                        fd=fd,
                        lock_path=str(lock_path),
                        task_id=task_id,
                    )
                )
            for lease in created:
                self.leases[lease.resource_id] = lease
            return [lease.resource_id for lease in created]
        except Exception:
            for lease in created:
                self._unlock_lease(lease)
            raise

    def _service_pending(self) -> None:
        if not self.pending:
            return
        now = self.now()
        retained: Deque[AcquireRequest] = deque()
        while self.pending:
            req = self.pending.popleft()
            if req.expires_monotonic < now:
                continue
            if req.agent_id not in self.agents:
                continue
            if self._can_grant(req.resources):
                self._grant_resources(
                    agent_id=req.agent_id,
                    resources=req.resources,
                    lease_ttl_ms=req.lease_ttl_ms,
                    task_id=req.task_id,
                )
            else:
                retained.append(req)
        self.pending = retained

    def _expire_leases(self) -> None:
        now = self.now()
        expired = [resource_id for resource_id, lease in self.leases.items() if lease.lease_until_monotonic < now]
        for resource_id in expired:
            lease = self.leases.pop(resource_id)
            self._unlock_lease(lease)

    def _expire_stale_agents(self) -> None:
        now = self.now()
        stale = [
            agent_id
            for agent_id, agent in self.agents.items()
            if (now - agent.last_seen_monotonic) * 1000 > self.heartbeat_timeout_ms
        ]
        for agent_id in stale:
            self.unregister_agent(agent_id)

    def _unlock_lease(self, lease: ResourceLease) -> None:
        with contextlib.suppress(OSError):
            fcntl.flock(lease.fd, fcntl.LOCK_UN)
        with contextlib.suppress(OSError):
            os.close(lease.fd)

    def _require_agent(self, agent_id: str) -> AgentRecord:
        if agent_id not in self.agents:
            raise ValueError(f"unknown agent_id: {agent_id}")
        return self.agents[agent_id]

    def _require_task(self, task_id: str) -> TaskRecord:
        if task_id not in self.tasks:
            raise ValueError(f"unknown task_id: {task_id}")
        return self.tasks[task_id]
