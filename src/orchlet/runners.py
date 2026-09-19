from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .artifacts import write_json, write_text
from .contracts import AgentBackend, Emit, Runner
from .errors import ConfigurationError, OutputValidationError, TaskCancelled
from .models import AgentRequest, ExecutionRequest, ExecutionResult


class CancellationToken:
    def __init__(self) -> None:
        self._event = asyncio.Event()

    def request(self) -> None:
        self._event.set()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()


@dataclass(frozen=True)
class TaskContext:
    task_id: str
    attempt: int
    cancellation: CancellationToken
    _emit: Emit

    def report(self, **metrics: Any) -> None:
        self._emit("metrics", metrics)


async def _cancellable[T](
    work: Awaitable[T], cancellation: CancellationToken, *, can_cancel: bool = True
) -> T:
    async def guarded() -> T:
        try:
            return await work
        except (SystemExit, KeyboardInterrupt) as exc:
            raise RuntimeError(f"Leaf execution raised {type(exc).__name__}: {exc}") from exc

    job = asyncio.create_task(guarded())
    watcher = asyncio.create_task(cancellation.wait())
    try:
        done, _ = await asyncio.wait((job, watcher), return_when=asyncio.FIRST_COMPLETED)
        if watcher in done and cancellation.requested:
            if can_cancel:
                job.cancel()
            # A running thread cannot be killed: retain its reservation until it exits.
            await asyncio.gather(job, return_exceptions=True)
            raise TaskCancelled("Execution cancelled after runner stopped")
        return await job
    finally:
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        if not job.done():
            if can_cancel:
                job.cancel()
            await asyncio.gather(job, return_exceptions=True)


class PythonRunner(Runner):
    async def execute(
        self, request: ExecutionRequest, emit: Emit, cancellation: CancellationToken
    ) -> ExecutionResult[Any]:
        fn, args = request.definition.function, request.args
        if request.definition.context:
            args = (TaskContext(request.task_id, request.attempt.number, cancellation, emit), *args)
        asynchronous = inspect.iscoroutinefunction(fn)
        work = (
            fn(*args, **request.kwargs)
            if asynchronous
            else asyncio.to_thread(fn, *args, **request.kwargs)
        )
        value = await _cancellable(work, cancellation, can_cancel=asynchronous)
        if inspect.isawaitable(value):
            # Don't leak a coroutine returned accidentally by a sync callable.
            if inspect.iscoroutine(value):
                value.close()
            raise TypeError("Declare coroutine tasks with 'async def'")
        if request.definition.validator is not None:
            value = request.definition.validator.validate(value, request.attempt)
        return ExecutionResult(value)


class AgentRunner(Runner):
    def __init__(self, backends: Mapping[str, AgentBackend]) -> None:
        self.backends: dict[str, AgentBackend] = dict(backends)

    async def execute(
        self, request: ExecutionRequest, emit: Emit, cancellation: CancellationToken
    ) -> ExecutionResult[Any]:
        definition = request.definition
        backend = (
            self.backends.get(definition.backend)
            if isinstance(definition.backend, str)
            else definition.backend
        )
        if backend is None:
            raise ConfigurationError(f"Unregistered agent backend: {definition.backend!r}")
        if (
            definition.session is None
            or definition.prompt_builder is None
            or definition.codec is None
        ):
            raise ConfigurationError(
                "Agent definitions require a session policy, prompt builder, and codec"
            )
        session_id = definition.session.bind(request.task_id, request.attempt)
        if session_id and not backend.supports_resume:
            raise ConfigurationError("This backend does not support continuing a session")
        prompt = await definition.prompt_builder.build(
            definition.function,
            request.args,
            request.kwargs,
            request.attempt,
        )
        if request.artifacts is not None:
            await write_text(request.artifacts.prompt_txt, prompt)
            await write_json(
                request.artifacts.launch_json,
                {
                    "backend": f"{type(backend).__module__}.{type(backend).__qualname__}",
                    "session_id": session_id,
                    "attempt": request.attempt.number,
                },
            )
        if cancellation.requested:
            raise TaskCancelled("Cancelled while preparing agent prompt")
        reply = await backend.run_turn(
            AgentRequest(
                prompt, request.task_id, request.attempt.number, session_id, request.artifacts
            ),
            emit,
            cancellation,
        )
        if request.artifacts is not None:
            artifacts = request.artifacts
            await write_text(artifacts.output_txt, reply.text)
            if not artifacts.stdout_log.exists():
                await write_text(artifacts.stdout_log, reply.stdout)
            if not artifacts.stderr_log.exists():
                await write_text(artifacts.stderr_log, reply.stderr)
        try:
            value = definition.codec.decode(reply.text)
            if definition.validator is not None:
                value = definition.validator.validate(value, request.attempt)
        except Exception as exc:
            raise OutputValidationError(
                str(exc),
                reply.text,
                stdout=reply.stdout,
                stderr=reply.stderr,
                session_id=reply.session_id,
                exit_code=reply.exit_code,
            ) from exc
        return ExecutionResult(
            value, reply.text, reply.stdout, reply.stderr, reply.session_id, reply.exit_code
        )


class SimulatedRunner(Runner):
    """Deterministic duration/result functions can be keyed by request inputs or metadata."""

    def __init__(
        self,
        result: Callable[[ExecutionRequest], Any] | object,
        duration: float | Callable[[ExecutionRequest], float] = 1.0,
    ) -> None:
        self.result = result
        self.duration = duration

    async def execute(
        self, request: ExecutionRequest, emit: Emit, cancellation: CancellationToken
    ) -> ExecutionResult[Any]:
        duration = self.duration(request) if callable(self.duration) else self.duration
        await _cancellable(request.clock.sleep(duration), cancellation)
        value = self.result(request) if callable(self.result) else self.result
        if request.definition.validator is not None:
            value = request.definition.validator.validate(value, request.attempt)
        return ExecutionResult(value)
