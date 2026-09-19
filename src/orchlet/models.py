"""Immutable values shared by runtime, schedulers, runners and storage adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping


def freeze(value):
    """Freeze JSON-like metadata recursively; opaque values must already be immutable."""
    if isinstance(value, Mapping):
        return MappingProxyType({k: freeze(v) for k, v in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(freeze(v) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(freeze(v) for v in value)
    return value


class TaskState(str, Enum):
    SUBMITTED = "submitted"
    WAITING = "waiting_dependencies"
    READY = "ready"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

    @property
    def terminal(self):
        return self in {self.SUCCEEDED, self.FAILED, self.SKIPPED, self.CANCELLED}


class Ordering(Enum):
    HIGHER = 1
    LOWER = -1
    EQUIVALENT = 0
    INCOMPARABLE = 2


class Gate(Enum):
    WAIT = "wait"
    PASS = "pass"
    IMPOSSIBLE = "impossible"


@dataclass(frozen=True)
class TaskView:
    id: str
    run_id: str
    scope_id: str
    name: str
    state: TaskState
    priority: Any = 0
    resources: Mapping[str, float] = field(default_factory=dict)
    metrics: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    dependencies: tuple[str, ...] = ()
    submitted_at: float = 0.0
    ready_at: float | None = None
    sequence: int = 0
    attempt: int = 0
    waiting_dependents: int = 0

    def __post_init__(self):
        for name in ("resources", "metrics", "metadata"):
            object.__setattr__(self, name, freeze(getattr(self, name)))


@dataclass(frozen=True)
class ResourceSnapshot:
    capacity: Mapping[str, float]
    available: Mapping[str, float]

    def __post_init__(self):
        object.__setattr__(self, "capacity", freeze(self.capacity))
        object.__setattr__(self, "available", freeze(self.available))


@dataclass(frozen=True)
class AllocationPlan:
    reservations: Mapping[str, Mapping[str, float]]
    remaining: ResourceSnapshot

    def __post_init__(self):
        object.__setattr__(self, "reservations", freeze(self.reservations))


@dataclass(frozen=True)
class ScheduleSnapshot:
    revision: int
    now: float
    ready: tuple[TaskView, ...]
    running: tuple[TaskView, ...]
    resources: ResourceSnapshot
    metrics: Mapping[str, Any] = field(default_factory=dict)
    trigger: str = "event"

    def __post_init__(self):
        object.__setattr__(self, "metrics", freeze(self.metrics))


@dataclass(frozen=True)
class Start:
    task_id: str
    reason: str = "selected"
    score: float | None = None


@dataclass(frozen=True)
class ScheduleDecision:
    revision: int
    starts: tuple[Start, ...] = ()
    wake_at: float | None = None


@dataclass(frozen=True)
class RuntimeEvent:
    sequence: int
    time: float
    kind: str
    run_id: str | None = None
    task_id: str | None = None
    data: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "data", freeze(self.data))


@dataclass(frozen=True)
class StateSnapshot:
    revision: int
    tasks: Mapping[str, TaskView]

    def __post_init__(self):
        object.__setattr__(self, "tasks", freeze(self.tasks))


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    delay: float = 0.0


@dataclass(frozen=True)
class Attempt:
    number: int
    started_at: float
    finished_at: float
    error: str | None = None
    raw_text: str | None = None


@dataclass(frozen=True)
class TaskResult:
    value: Any
    attempts: tuple[Attempt, ...]
    raw_text: str | None = None
    stdout: str = ""
    stderr: str = ""
    session_id: str | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class ExecutionResult:
    value: Any
    raw_text: str | None = None
    stdout: str = ""
    stderr: str = ""
    session_id: str | None = None


@dataclass(frozen=True)
class Outcome:
    task_id: str
    value: Any = None
    error: BaseException | None = None

    @property
    def succeeded(self):
        return self.error is None


@dataclass(frozen=True)
class AgentReply:
    text: str
    stdout: str = ""
    stderr: str = ""
    session_id: str | None = None


@dataclass(frozen=True)
class AgentRequest:
    prompt: str
    task_id: str
    attempt: int
    session_id: str | None = None


@dataclass(frozen=True)
class AttemptContext:
    task_id: str
    number: int
    previous_error: str | None = None
    previous_output: str | None = None


@dataclass(frozen=True)
class ExecutionRequest:
    task_id: str
    definition: Any
    args: tuple
    kwargs: Mapping[str, Any]
    attempt: AttemptContext
    clock: Any
