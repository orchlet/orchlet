from __future__ import annotations

import asyncio
from collections.abc import Generator, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar, cast

from ._bridges import Completion, RuntimeBridge
from .models import TaskResult, TaskView

if TYPE_CHECKING:
    from .definitions import TaskDef
    from .runtime import SubmitOptions

# Handles only expose their result type; mutable completion state stays internal.
T_co = TypeVar("T_co", covariant=True)


def quiet_future[T](future: asyncio.Future[T]) -> asyncio.Future[T]:
    """Failures remain available to awaiters/scope checks, without un-retrieved warnings."""

    def retrieve(done: asyncio.Future[T]) -> None:
        if not done.cancelled():
            done.exception()

    future.add_done_callback(retrieve)
    return future


async def await_shared[T](future: asyncio.Future[T]) -> T:
    """Detach a cancelled waiter without cancelling or installing logging on shared work."""
    if future.done():
        return future.result()
    waiter: asyncio.Future[T] = asyncio.get_running_loop().create_future()

    def forward(done: asyncio.Future[T]) -> None:
        if waiter.done():
            return
        if done.cancelled():
            waiter.cancel()
        elif (error := done.exception()) is not None:
            waiter.set_exception(error)
        else:
            waiter.set_result(done.result())

    future.add_done_callback(forward)
    try:
        return await waiter
    finally:
        future.remove_done_callback(forward)


@dataclass(frozen=True)
class OutputRef[T_co]:
    task_id: str
    run_id: str


class TaskHandle(Generic[T_co]):
    def __init__(
        self,
        runtime: RuntimeBridge,
        task_id: str,
        run_id: str,
        completion: Completion[T_co],
        *,
        key: str | None = None,
    ) -> None:
        self._runtime = runtime
        self.id: str = task_id
        self.run_id: str = run_id
        self.key: str | None = key
        self._completion: Final = completion

    async def result(self) -> T_co:
        # Cancelling an awaiter does not silently cancel shared node execution.
        try:
            value = await await_shared(self._completion.future)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._completion.observed = True
            raise
        self._completion.observed = True
        return value

    def __await__(self) -> Generator[Any, None, T_co]:
        return self.result().__await__()

    @property
    def done(self) -> bool:
        return self._completion.future.done()

    @property
    def snapshot(self) -> TaskView | None:
        return self._runtime.state.snapshot().tasks.get(self.id)

    @property
    def details(self) -> TaskResult[T_co] | None:
        return self._runtime.details(self.id)

    @property
    def artifacts_dir(self) -> Path:
        return self._runtime.artifacts.task_dir(self.run_id, self.id)

    async def cancel(self) -> None:
        await self._runtime.command("cancel_task", self.run_id, self.id)

    async def update_metrics(self, **metrics: Any) -> None:
        await self._runtime.command("metrics", self.run_id, self.id, metrics)


class FlowHandle(Generic[T_co]):
    def __init__(
        self,
        runtime: RuntimeBridge,
        scope_id: str,
        run_id: str,
        completion: Completion[T_co],
        *,
        key: str | None = None,
    ) -> None:
        self._runtime = runtime
        self.id: str = scope_id
        self.run_id: str = run_id
        self.key: str | None = key
        self._completion: Final = completion

    async def result(self) -> T_co:
        try:
            value = await await_shared(self._completion.future)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._completion.observed = True
            raise
        self._completion.observed = True
        return value

    def __await__(self) -> Generator[Any, None, T_co]:
        return self.result().__await__()

    @property
    def done(self) -> bool:
        return self._completion.future.done()

    async def cancel(self) -> None:
        """Request cancellation; await the handle to wait for descendant cleanup."""
        if not self.done:
            await self._runtime.command("cancel_scope", self.run_id, self.id)


def observe_handles[T](
    runtime: RuntimeBridge, run_id: str, handles: Iterable[TaskHandle[T] | FlowHandle[T]]
) -> tuple[TaskHandle[T] | FlowHandle[T], ...]:
    """Validate the entire collection before taking responsibility for its errors."""
    members = tuple(handles)
    for handle in members:
        if not isinstance(cast(object, handle), (TaskHandle, FlowHandle)):
            raise TypeError("Expected a task or flow handle")
        if not runtime.owns(run_id, handle):
            raise ValueError("Handles must belong to this Runtime and run")
    for handle in members:
        runtime.completion(handle.id).observed = True
    return members


class RunHandle(Generic[T_co]):
    def __init__(self, runtime: RuntimeBridge, run_id: str, completion: Completion[T_co]) -> None:
        self._runtime = runtime
        self.id: str = run_id
        self._completion: Final = completion

    @property
    def done(self) -> bool:
        return self._completion.future.done()

    @property
    def artifacts_dir(self) -> Path:
        return self._runtime.artifacts.run_dir(self.id)

    async def wait(self) -> T_co:
        return await await_shared(self._completion.future)

    def __await__(self) -> Generator[Any, None, T_co]:
        return self.wait().__await__()

    def submit[T](
        self,
        definition: TaskDef[..., T],
        *args: Any,
        options: SubmitOptions | None = None,
        **kwargs: Any,
    ) -> TaskHandle[T]:
        return self._runtime.submit(self.id, None, definition, args, kwargs, options)

    async def close_inputs(self) -> None:
        await self._runtime.command("close_inputs", self.id)

    async def cancel(self) -> None:
        await self._runtime.command("cancel_run", self.id)

    async def update_metrics(self, **metrics: Any) -> None:
        await self._runtime.command("metrics", self.id, payload=metrics)

    @property
    def tasks(self) -> tuple[TaskView, ...]:
        return tuple(
            t for t in self._runtime.state.snapshot().tasks.values() if t.run_id == self.id
        )

    def task(self, task_id: str) -> TaskHandle[Any]:
        handle = self._runtime.task(task_id)
        if handle.run_id != self.id:
            raise KeyError(task_id)
        return handle
