from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


@dataclass
class AgentRecord:
    agent_id: str
    name: str
    capabilities: Set[str]
    metadata: Dict[str, Any]
    last_seen_monotonic: float
    pid: Optional[int] = None
    current_task_id: Optional[str] = None


@dataclass
class ResourceLease:
    resource_id: str
    owner_agent_id: str
    lease_until_monotonic: float
    fd: int
    lock_path: str
    task_id: Optional[str] = None


@dataclass
class AcquireRequest:
    request_id: str
    agent_id: str
    resources: List[str]
    lease_ttl_ms: int
    expires_monotonic: float
    task_id: Optional[str] = None


@dataclass
class TaskRecord:
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    priority: int
    required_capabilities: Set[str]
    required_resources: List[str]
    lease_ttl_ms: int
    created_monotonic: float
    status: str = "queued"
    assigned_agent_id: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    labels: Set[str] = field(default_factory=set)

