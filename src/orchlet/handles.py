from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


def quiet_future(future):
    """Failures remain available to awaiters/scope checks, without un-retrieved warnings."""

    def retrieve(done):
        if not done.cancelled():
            done.exception()

    future.add_done_callback(retrieve)
    return future


async def await_shared(future):
    """Detach a cancelled waiter without cancelling or installing logging on shared work."""
    if future.done():
        return future.result()
    waiter = asyncio.get_running_loop().create_future()

    def forward(done):
        if waiter.done():
            return
        if done.cancelled():
            waiter.cancel()
        elif done.exception() is not None:
            waiter.set_exception(done.exception())
        else:
            waiter.set_result(done.result())

    future.add_done_callback(forward)
    try:
        return await waiter
    finally:
        future.remove_done_callback(forward)


@dataclass(frozen=True)
class OutputRef:
    task_id: str
    run_id: str


class TaskHandle(Generic[T]):
    def __init__(self, runtime, task_id, run_id):
        self._runtime = runtime
        self.id, self.run_id = task_id, run_id
        self._future = quiet_future(asyncio.get_running_loop().create_future())
        self._admitted = quiet_future(asyncio.get_running_loop().create_future())
        self._observed = False

    async def result(self) -> T:
        # Cancelling an awaiter does not silently cancel shared node execution.
        try:
            value = await await_shared(self._future)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._observed = True
            raise
        self._observed = True
        return value

    def __await__(self):
        return self.result().__await__()

    @property
    def done(self):
        return self._future.done()

    @property
    def snapshot(self):
        return self._runtime.state.snapshot().tasks.get(self.id)

    @property
    def details(self):
        return self._runtime.result_details(self.id)

    async def cancel(self):
        await self._runtime._command("cancel_task", self.run_id, self.id)

    async def update_metrics(self, **metrics):
        await self._runtime._command("metrics", self.run_id, self.id, metrics)


class FlowHandle:
    def __init__(self, scope_id):
        self.id = scope_id
        self._future = quiet_future(asyncio.get_running_loop().create_future())
        self._observed = False

    async def result(self):
        try:
            value = await await_shared(self._future)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self._observed = True
            raise
        self._observed = True
        return value

    def __await__(self):
        return self.result().__await__()


class RunHandle:
    def __init__(self, runtime, run_id):
        self._runtime, self.id = runtime, run_id
        self._future = quiet_future(asyncio.get_running_loop().create_future())

    @property
    def done(self):
        return self._future.done()

    async def wait(self):
        return await await_shared(self._future)

    def __await__(self):
        return self.wait().__await__()

    def submit(self, definition, *args, **kwargs):
        return self._runtime._submit(self.id, None, definition, args, kwargs)

    async def close_inputs(self):
        await self._runtime._command("close_inputs", self.id)

    async def cancel(self):
        await self._runtime._command("cancel_run", self.id)

    async def update_metrics(self, **metrics):
        await self._runtime._command("metrics", self.id, payload=metrics)

    @property
    def tasks(self):
        return tuple(
            t for t in self._runtime.state.snapshot().tasks.values() if t.run_id == self.id
        )

    def task(self, task_id):
        handle = self._runtime._handles[task_id]
        if handle.run_id != self.id:
            raise KeyError(task_id)
        return handle
