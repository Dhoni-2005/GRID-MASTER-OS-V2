"""
grid/models.py — Grid Master OS Phase 7
Wire-protocol dataclasses for all inter-node payloads.
No external imports — stdlib dataclasses and typing only.
HMAC signing logic lives in grid.signing, NOT here.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


# ── NODE INFO ─────────────────────────────────────────────────

@dataclass
class NodeInfo:
    """Identity and capability descriptor sent by a worker on registration."""
    node_id: str
    platform: str               # "render" | "huggingface" | "local"
    capabilities: list[str]
    registered_at: str          # ISO-8601
    public_url: str = ""
    worker_port: int = 8001

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "NodeInfo":
        required = ("node_id", "platform", "capabilities", "registered_at")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"NodeInfo missing required fields: {missing}")
        return cls(
            node_id       = str(d["node_id"]),
            platform      = str(d["platform"]),
            capabilities  = list(d["capabilities"]),
            registered_at = str(d["registered_at"]),
            public_url    = str(d.get("public_url", "")),
            worker_port   = int(d.get("worker_port", 8001)),
        )


# ── HEARTBEAT ─────────────────────────────────────────────────

@dataclass
class HeartbeatPayload:
    """Worker → Master liveness and load signal."""
    node_id: str
    timestamp_utc: str          # ISO-8601
    active_task_ids: list[int]
    cpu_percent: float | None = None
    memory_percent: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "HeartbeatPayload":
        required = ("node_id", "timestamp_utc", "active_task_ids")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"HeartbeatPayload missing required fields: {missing}")
        return cls(
            node_id         = str(d["node_id"]),
            timestamp_utc   = str(d["timestamp_utc"]),
            active_task_ids = [int(x) for x in d["active_task_ids"]],
            cpu_percent     = float(d["cpu_percent"]) if d.get("cpu_percent") is not None else None,
            memory_percent  = float(d["memory_percent"]) if d.get("memory_percent") is not None else None,
        )


# ── TASK ASSIGNMENT ───────────────────────────────────────────

@dataclass
class TaskAssignment:
    """Master → Worker task dispatch payload. Signature set by grid.signing."""
    task_id: int
    project_id: int
    title: str
    input_data: str
    priority: int
    assigned_at: str            # ISO-8601
    signature: str = ""         # HMAC-SHA256 hex; set by dispatcher after construction

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TaskAssignment":
        required = ("task_id", "project_id", "title", "input_data", "priority", "assigned_at")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"TaskAssignment missing required fields: {missing}")
        return cls(
            task_id     = int(d["task_id"]),
            project_id  = int(d["project_id"]),
            title       = str(d["title"]),
            input_data  = str(d["input_data"]),
            priority    = int(d["priority"]),
            assigned_at = str(d["assigned_at"]),
            signature   = str(d.get("signature", "")),
        )


# ── RESULT PAYLOAD ────────────────────────────────────────────

@dataclass
class ResultPayload:
    """Worker → Master task result report. Signature set by grid.signing."""
    task_id: int
    node_id: str
    status: str                 # "completed" | "failed" | "rejected" | "interrupted"
    output: str
    completed_at: str           # ISO-8601
    error: str | None = None
    signature: str = ""         # HMAC-SHA256 hex; set by worker_runtime after construction

    VALID_STATUSES: tuple[str, ...] = field(
        default=("completed", "failed", "rejected", "interrupted"),
        init=False, repr=False, compare=False,
    )

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("VALID_STATUSES", None)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ResultPayload":
        required = ("task_id", "node_id", "status", "output", "completed_at")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"ResultPayload missing required fields: {missing}")
        return cls(
            task_id      = int(d["task_id"]),
            node_id      = str(d["node_id"]),
            status       = str(d["status"]),
            output       = str(d["output"]),
            completed_at = str(d["completed_at"]),
            error        = str(d["error"]) if d.get("error") else None,
            signature    = str(d.get("signature", "")),
        )


# ── POLL RESPONSE ─────────────────────────────────────────────

@dataclass
class PollResponse:
    """Master → Worker poll response. Contains an assignment or a wait signal."""
    has_work: bool
    wait_seconds: int = 5
    assignment: TaskAssignment | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "has_work":     self.has_work,
            "wait_seconds": self.wait_seconds,
            "assignment":   self.assignment.to_dict() if self.assignment else None,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PollResponse":
        if "has_work" not in d:
            raise ValueError("PollResponse missing required field: has_work")
        assignment = None
        if d.get("assignment"):
            assignment = TaskAssignment.from_dict(d["assignment"])
        return cls(
            has_work     = bool(d["has_work"]),
            wait_seconds = int(d.get("wait_seconds", 5)),
            assignment   = assignment,
        )


# ── REGISTRATION RESPONSE ─────────────────────────────────────

@dataclass
class RegistrationResponse:
    """Master → Worker registration acknowledgement."""
    node_id: str
    registration_token: str
    token_expires_at: str       # ISO-8601
    master_version: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RegistrationResponse":
        required = ("node_id", "registration_token", "token_expires_at", "master_version")
        missing = [k for k in required if k not in d]
        if missing:
            raise ValueError(f"RegistrationResponse missing required fields: {missing}")
        return cls(
            node_id             = str(d["node_id"]),
            registration_token  = str(d["registration_token"]),
            token_expires_at    = str(d["token_expires_at"]),
            master_version      = str(d["master_version"]),
        )
