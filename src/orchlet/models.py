"""Immutable values shared by runtime, schedulers, runners and storage adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping, cast

if TYPE_CHECKING:
    from .contracts import Clock
    from .definitions import TaskDef


def freeze(value: Any) -> Any:
    """Freeze JSON-like metadata recursively; opaque values must already be immutable."""
    if isinstance(value, Mapping):
        return MappingProxyType({k: freeze(v) for k, v in cast(Mapping[Any, Any], value).items()})
    if isinstance(value, (tuple, list)):
        return tuple(freeze(v) for v in cast(tuple[Any, ...] | list[Any], value))
    if isinstance(value, (set, frozenset)):
        return frozenset(freeze(v) for v in cast(set[Any] | frozenset[Any], value))
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
    def terminal(self) -> bool:
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
    resources: Mapping[str, float] = field(default_factory=lambda: dict[str, float]())
    metrics: Mapping[str, Any] = field(default_factory=lambda: dict[str, Any]())
    metadata: Mapping[str, Any] = field(default_factory=lambda: dict[str, Any]())
    dependencies: tuple[str, ...] = ()
    submitted_at: float = 0.0
    ready_at: float | None = None
    sequence: int = 0
    attempt: int = 0
    waiting_dependents: int = 0

    def __post_init__(self) -> None:
        for name in ("resources", "metrics", "metadata"):
            object.__setattr__(self, name, freeze(getattr(self, name)))


@dataclass(frozen=True)
class ResourceSnapshot:
    capacity: Mapping[str, float]
    available: Mapping[str, float]

    def __post_init__(self) -> None:
        object.__setattr__(self, "capacity", freeze(self.capacity))
        object.__setattr__(self, "available", freeze(self.available))


@dataclass(frozen=True)
class AllocationPlan:
    reservations: Mapping[str, Mapping[str, float]]
    remaining: ResourceSnapshot

    def __post_init__(self) -> None:
        object.__setattr__(self, "reservations", freeze(self.reservations))


@dataclass(frozen=True)
class ScheduleSnapshot:
    revision: int
    now: float
    ready: tuple[TaskView, ...]
    running: tuple[TaskView, ...]
    resources: ResourceSnapshot
    metrics: Mapping[str, Any] = field(default_factory=lambda: dict[str, Any]())
    trigger: str = "event"

    def __post_init__(self) -> None:
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
    data: Mapping[str, Any] = field(default_factory=lambda: dict[str, Any]())

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", freeze(self.data))


@dataclass(frozen=True)
class StateSnapshot:
    revision: int
    tasks: Mapping[str, TaskView]

    def __post_init__(self) -> None:
        object.__setattr__(self, "tasks", freeze(self.tasks))


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    delay: float = 0.0


@dataclass(frozen=True)
class AttemptArtifacts:
    directory: Path

    @property
    def prompt_txt(self) -> Path:
        return self.directory / "prompt.txt"

    @property
    def stdout_log(self) -> Path:
        return self.directory / "stdout.log"

    @property
    def stderr_log(self) -> Path:
        return self.directory / "stderr.log"

    @property
    def output_txt(self) -> Path:
        return self.directory / "output.txt"

    @property
    def launch_json(self) -> Path:
        return self.directory / "launch.json"

    @property
    def result_json(self) -> Path:
        return self.directory / "result.json"


@dataclass(frozen=True)
class Attempt:
    number: int
    started_at: float
    finished_at: float
    error: str | None = None
    raw_text: str | None = None
    stdout: str = ""
    stderr: str = ""
    session_id: str | None = None
    exit_code: int | None = None
    artifacts: AttemptArtifacts | None = None


@dataclass(frozen=True)
class TaskResult[T_co]:
    value: T_co | None
    attempts: tuple[Attempt, ...]
    raw_text: str | None = None
    stdout: str = ""
    stderr: str = ""
    session_id: str | None = None
    error: BaseException | None = None
    artifacts: AttemptArtifacts | None = None
    exit_code: int | None = None


@dataclass(frozen=True)
class ExecutionResult[T_co]:
    value: T_co
    raw_text: str | None = None
    stdout: str = ""
    stderr: str = ""
    session_id: str | None = None
    exit_code: int | None = None


@dataclass(frozen=True)
class Outcome[T_co]:
    task_id: str
    value: T_co | None = None
    error: BaseException | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class AgentReply:
    text: str
    stdout: str = ""
    stderr: str = ""
    session_id: str | None = None
    exit_code: int | None = None


@dataclass(frozen=True)
class AgentRequest:
    prompt: str
    task_id: str
    attempt: int
    session_id: str | None = None
    artifacts: AttemptArtifacts | None = None


@dataclass(frozen=True)
class AttemptContext:
    task_id: str
    number: int
    previous_error: str | None = None
    previous_output: str | None = None


@dataclass(frozen=True)
class ExecutionRequest:
    task_id: str
    definition: TaskDef[..., Any]
    args: tuple[Any, ...]
    kwargs: Mapping[str, Any]
    attempt: AttemptContext
    clock: Clock
    artifacts: AttemptArtifacts | None = None
