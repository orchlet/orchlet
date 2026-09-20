"""Extension points. Policies return decisions; only Runtime commits state changes."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Concatenate, Protocol

from .models import (
    AgentReply,
    AgentRequest,
    AllocationPlan,
    Attempt,
    AttemptContext,
    BatchDecision,
    BatchSnapshot,
    ExecutionRequest,
    ExecutionResult,
    Gate,
    Ordering,
    ResourceSnapshot,
    RetryDecision,
    RuntimeEvent,
    ScheduleDecision,
    ScheduleSnapshot,
    StateSnapshot,
    TaskState,
    TaskView,
)

if TYPE_CHECKING:
    from .definitions import FlowDef
    from .handles import OutputRef, RunHandle, TaskHandle
    from .runners import CancellationToken
    from .runtime import FlowContext

type Emit = Callable[[str, Mapping[str, Any]], None]


class Runtime(ABC):
    @abstractmethod
    async def arun[**P, T](
        self,
        flow: FlowDef[P, T] | Callable[Concatenate[FlowContext, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T: ...

    @abstractmethod
    def start[T](
        self,
        flow: FlowDef[..., T] | Callable[..., Awaitable[T]],
        *args: Any,
        keep_open: bool = False,
        **kwargs: Any,
    ) -> RunHandle[T]: ...

    def run[**P, T](
        self,
        flow: FlowDef[P, T] | Callable[Concatenate[FlowContext, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.arun(flow, *args, **kwargs))
        raise RuntimeError("Use 'await runtime.arun(...)' inside an event loop")


class FlowController[T_co](ABC):
    @abstractmethod
    async def run(
        self, context: FlowContext, args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> T_co: ...


class InputResolver(ABC):
    @abstractmethod
    def references(self, value: Any) -> Sequence[TaskHandle[Any] | OutputRef[Any]]: ...

    @abstractmethod
    def capture(self, value: Any) -> Any: ...

    @abstractmethod
    def resolve(self, value: Any, results: Mapping[str, Any]) -> Any: ...


class DependencyPolicy(ABC):
    @abstractmethod
    def evaluate(self, states: Sequence[TaskState]) -> Gate: ...


class PriorityOrder(ABC):
    @abstractmethod
    def compare(self, a: Any, b: Any) -> Ordering: ...


class WeightModel(ABC):
    @abstractmethod
    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float: ...


class Scheduler(ABC):
    @abstractmethod
    def schedule(self, snapshot: ScheduleSnapshot) -> ScheduleDecision: ...

    def observe(self, event: RuntimeEvent) -> None:
        """Optional sequential observation of runtime events."""

    def bind_resources(self, resources: ResourceAllocator) -> None:
        """Optional binding to the allocator used to validate actual reservations."""


class ReadyIndex(ABC):
    @abstractmethod
    def update(self, tasks: Sequence[TaskView]) -> None: ...

    @abstractmethod
    def candidates(self) -> Sequence[TaskView]: ...


class ResourceAllocator(ABC):
    @abstractmethod
    def initial(self) -> ResourceSnapshot: ...

    @abstractmethod
    def requirements(self, task: TaskView) -> Mapping[str, float]: ...

    @abstractmethod
    def plan(self, tasks: Sequence[TaskView], resources: ResourceSnapshot) -> AllocationPlan | None:
        """Return an AllocationPlan, or None when the batch cannot fit."""


class AdmissionPolicy(ABC):
    @abstractmethod
    def admit(self, task: TaskView, outstanding: int) -> bool: ...


class RetryPolicy(ABC):
    @abstractmethod
    def decide(self, failure: BaseException, history: Sequence[Attempt]) -> RetryDecision: ...


class FailurePolicy(ABC):
    @abstractmethod
    def fail_scope(self, unobserved_errors: Sequence[BaseException]) -> bool: ...


class BatchFailurePolicy(ABC):
    """Decide whether to admit more members and whether to cancel pending members."""

    @abstractmethod
    def decide(self, snapshot: BatchSnapshot) -> BatchDecision: ...


class Runner(ABC):
    @abstractmethod
    async def execute(
        self,
        request: ExecutionRequest,
        emit: Emit,
        cancellation: CancellationToken,
    ) -> ExecutionResult[Any]: ...


class AgentBackend(ABC):
    supports_resume = False

    @abstractmethod
    async def run_turn(
        self,
        request: AgentRequest,
        emit: Emit,
        cancellation: CancellationToken,
    ) -> AgentReply: ...


class PromptBuilder(ABC):
    @abstractmethod
    async def build(
        self,
        function: Callable[..., str | Awaitable[str]],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        attempt: AttemptContext,
    ) -> str: ...


class OutputCodec[T_co](ABC):
    @abstractmethod
    def decode(self, raw: str) -> T_co: ...


class ResultValidator[T_co](ABC):
    @abstractmethod
    def validate(self, value: Any, context: AttemptContext) -> T_co:
        """Return a validated value or raise; never silently ignore invalid output."""


class SessionPolicy(ABC):
    @abstractmethod
    def bind(self, task_id: str, attempt: AttemptContext) -> str | None: ...

    @abstractmethod
    def resource_key(self) -> str | None: ...


class StateStore(ABC):
    @abstractmethod
    def snapshot(self) -> StateSnapshot: ...

    @abstractmethod
    async def commit(
        self, expected_revision: int, changes: Mapping[str, TaskView]
    ) -> StateSnapshot: ...


class EventTransport[M](ABC):
    @abstractmethod
    def open(self) -> None:
        """Bind/reset the local nonblocking ingress for a new engine lifetime."""

    @abstractmethod
    def send(self, message: M) -> None: ...

    @abstractmethod
    async def receive(self) -> M: ...

    @abstractmethod
    def drain(self, limit: int) -> Sequence[M]: ...

    @abstractmethod
    def empty(self) -> bool: ...


class EventJournal(ABC):
    @abstractmethod
    async def append(self, event: RuntimeEvent) -> None: ...

    @abstractmethod
    def read(self) -> Sequence[RuntimeEvent]: ...


class ArtifactStore(ABC):
    """Locate durable execution artifacts and retain the run's event history."""

    @abstractmethod
    def run_dir(self, run_id: str) -> Path: ...

    @abstractmethod
    def task_dir(self, run_id: str, task_id: str) -> Path: ...

    @abstractmethod
    def attempt_dir(self, run_id: str, task_id: str, attempt: int) -> Path: ...

    @abstractmethod
    async def append_event(self, run_id: str, event: RuntimeEvent) -> None: ...

    @abstractmethod
    def open_run(self, key: str, *, resume: bool) -> tuple[str, bool]:
        """Exclusively open an unfinished matching run, or create a new run."""

    @abstractmethod
    def close_run(self, run_id: str) -> None: ...


class CheckpointCodec(ABC):
    """Serialize successful task values without losing their Python types."""

    @abstractmethod
    def encode(self, value: Any) -> bytes: ...

    @abstractmethod
    def decode(self, data: bytes) -> Any: ...


class Timer(Protocol):
    def cancel(self) -> None: ...


class Clock(ABC):
    @abstractmethod
    def now(self) -> float: ...

    @abstractmethod
    def schedule_at(self, when: float, callback: Callable[[], None]) -> Timer: ...

    async def sleep(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("delay must be nonnegative")
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def wake() -> None:
            if not future.done():
                future.set_result(None)

        timer = self.schedule_at(self.now() + delay, wake)
        try:
            await future
        finally:
            timer.cancel()
