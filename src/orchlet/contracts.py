"""Extension points. Policies return decisions; only Runtime commits state changes."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .models import (
    AgentReply,
    AgentRequest,
    AllocationPlan,
    Attempt,
    AttemptContext,
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


class Runtime(ABC):
    @abstractmethod
    async def arun(self, flow, *args, **kwargs): ...

    @abstractmethod
    def start(self, flow, *args, keep_open=False, **kwargs): ...

    def run(self, flow, *args, **kwargs):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.arun(flow, *args, **kwargs))
        raise RuntimeError("Use 'await runtime.arun(...)' inside an event loop")


class FlowController(ABC):
    @abstractmethod
    async def run(self, context, args, kwargs): ...


class InputResolver(ABC):
    @abstractmethod
    def references(self, value): ...

    @abstractmethod
    def capture(self, value): ...

    @abstractmethod
    def resolve(self, value, results): ...


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

    def bind_resources(self, resources):
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


class Runner(ABC):
    @abstractmethod
    async def execute(
        self,
        request: ExecutionRequest,
        emit: Callable[[str, Mapping], None],
        cancellation: Any,
    ) -> ExecutionResult: ...


class AgentBackend(ABC):
    supports_resume = False

    @abstractmethod
    async def run_turn(
        self,
        request: AgentRequest,
        emit: Callable[[str, Mapping], None],
        cancellation: Any,
    ) -> AgentReply: ...


class PromptBuilder(ABC):
    @abstractmethod
    async def build(
        self, function: Callable, args: tuple, kwargs: Mapping, attempt: AttemptContext
    ) -> str: ...


class OutputCodec(ABC):
    @abstractmethod
    def decode(self, raw: str) -> Any: ...


class ResultValidator(ABC):
    @abstractmethod
    def validate(self, value: Any, context: AttemptContext) -> Any:
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


class EventTransport(ABC):
    @abstractmethod
    def open(self):
        """Bind/reset the local nonblocking ingress for a new engine lifetime."""

    @abstractmethod
    def send(self, message): ...

    @abstractmethod
    async def receive(self): ...

    @abstractmethod
    def drain(self, limit): ...

    @abstractmethod
    def empty(self): ...


class EventJournal(ABC):
    @abstractmethod
    async def append(self, event: RuntimeEvent) -> None: ...

    @abstractmethod
    def read(self) -> Sequence[RuntimeEvent]: ...


class Clock(ABC):
    @abstractmethod
    def now(self) -> float: ...

    @abstractmethod
    def schedule_at(self, when, callback): ...

    async def sleep(self, delay):
        if delay < 0:
            raise ValueError("delay must be nonnegative")
        future = asyncio.get_running_loop().create_future()

        def wake():
            if not future.done():
                future.set_result(None)

        timer = self.schedule_at(self.now() + delay, wake)
        try:
            await future
        finally:
            timer.cancel()
