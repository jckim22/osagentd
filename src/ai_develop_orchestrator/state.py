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
            if task.required_capabilities and not task.required_capabilities.issubset(advertised):
                continue
            if not self._can_grant(task.required_resources):
                continue
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
        self.task_queue = deque(item for item in self.task_queue if item != task_id)
        self._persist_state()
        return task

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
        return {
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
                    "status": task.status,
                    "priority": task.priority,
                    "assigned_agent_id": task.assigned_agent_id,
                    "required_capabilities": sorted(task.required_capabilities),
                    "required_resources": task.required_resources,
                    "labels": sorted(task.labels),
                    "result": task.result,
                }
                for task in sorted(self.tasks.values(), key=lambda item: item.created_monotonic)
            ],
        }

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

