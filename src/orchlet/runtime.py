"""Single-writer, event-driven runtime. No worker waits on a dispatch semaphore."""

from __future__ import annotations

import asyncio
import contextvars
import itertools
import math
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from types import TracebackType
from urllib.parse import quote
from typing import Any, Concatenate, Self, cast

from ._bridges import Completion, RuntimeBridge
from .artifacts import (
    FileArtifactStore,
    atomic_write,
    attempt_metadata,
    json_bytes,
    read_json,
    read_text,
    result_metadata,
    write_json,
    write_text,
)
from .checkpoints import PickleCheckpointCodec, callable_identity, fingerprint
from .clocks import MonotonicClock
from .contracts import (
    AdmissionPolicy,
    AgentBackend,
    ArtifactStore,
    CheckpointCodec,
    Clock,
    DependencyPolicy,
    EventJournal,
    EventTransport,
    FailurePolicy,
    InputResolver,
    ResourceAllocator,
    RetryPolicy,
    Runner,
    Runtime,
    Scheduler,
    StateStore,
    Timer,
)
from .definitions import CoroutineFlowController, FlowDef, TaskDef
from .errors import (
    AdmissionError,
    ConfigurationError,
    DependencyFailed,
    RunClosedError,
    RecoveryError,
    SchedulingError,
    TaskCancelled,
    TaskFailed,
)
from .events import AsyncioEventTransport, MemoryEventJournal
from .handles import FlowHandle, OutputRef, RunHandle, TaskHandle, await_shared, quiet_future
from .inputs import NestedInputResolver
from .logging import log_event, log_runtime_error
from .models import (
    Attempt,
    AttemptArtifacts,
    AttemptContext,
    ExecutionRequest,
    ExecutionResult,
    Gate,
    Outcome,
    ResourceSnapshot,
    RuntimeEvent,
    ScheduleDecision,
    ScheduleSnapshot,
    Start,
    TaskResult,
    TaskState,
    TaskView,
)
from .policies import AllSuccessful, BoundedAdmission, FailScope, NoRetry
from .resources import TokenResourceAllocator
from .runners import AgentRunner, CancellationToken, PythonRunner
from .schedulers import PartialOrderScheduler
from .state import MemoryStateStore

_current_scope: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "orchlet_scope", default=None
)


@dataclass(frozen=True)
class SubmitOptions:
    after: tuple[TaskHandle[Any], ...] = ()
    dependency_policy: DependencyPolicy | None = None


@dataclass
class _Message:
    kind: str
    run_id: str | None = None
    task_id: str | None = None
    payload: Any = None
    ack: asyncio.Future[None] | None = None


@dataclass
class _Record:
    handle: TaskHandle[Any]
    definition: TaskDef[..., Any]
    scope_id: str | None
    inputs: Any
    data_dependencies: tuple[str, ...]
    control_dependencies: tuple[str, ...]
    policy: DependencyPolicy
    deferred: bool = False
    admitted: bool = False
    state: TaskState = TaskState.SUBMITTED
    submitted_at: float = 0.0
    ready_at: float | None = None
    sequence: int = 0
    metrics: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())
    attempts: list[Attempt] = field(default_factory=lambda: list[Attempt]())
    attempt: int = 0
    started_at: float = 0.0
    previous_error: str | None = None
    previous_output: str | None = None
    worker: asyncio.Task[None] | None = None
    token: CancellationToken = field(default_factory=CancellationToken)
    timer: Timer | None = None
    leases: dict[str, float] = field(default_factory=lambda: dict[str, float]())
    cancel_requested: bool = False
    timed_out: bool = False
    result: TaskResult[Any] | None = None
    error: BaseException | None = None
    signature: str = ""
    cached: dict[str, Any] | None = None
    resolved_signature: str | None = None
    value_digest: str | None = None

    def view(self) -> TaskView:
        resources = dict(self.definition.resources)
        if self.definition.session is not None:
            key = self.definition.session.resource_key()
            if key:
                resources[key] = 1
        return TaskView(
            id=self.handle.id,
            run_id=self.handle.run_id,
            scope_id=self.scope_id or f"{self.handle.run_id}/external",
            name=self.definition.name,
            state=self.state,
            priority=self.definition.priority,
            resources=resources,
            metrics=self.metrics,
            metadata=self.definition.metadata,
            dependencies=tuple(
                dict.fromkeys((*self.data_dependencies, *self.control_dependencies))
            ),
            submitted_at=self.submitted_at,
            ready_at=self.ready_at,
            sequence=self.sequence,
            attempt=self.attempt,
        )


@dataclass
class _Scope:
    id: str
    run_id: str
    handle: FlowHandle[Any]
    parent: str | None = None
    tasks: list[str] = field(default_factory=lambda: list[str]())
    children: list[str] = field(default_factory=lambda: list[str]())
    worker: asyncio.Task[None] | None = None
    flow_done: bool = False
    settled: bool = False
    value: Any = None
    error: BaseException | None = None


@dataclass
class _Run:
    handle: RunHandle[Any]
    root: str
    keep_open: bool
    external: list[str] = field(default_factory=lambda: list[str]())
    metrics: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())
    cancelled: bool = False


class FlowContext:
    def __init__(self, runtime: RuntimeBridge, run_id: str, scope_id: str) -> None:
        self._runtime = runtime
        self.run_id: str = run_id
        self.scope_id: str = scope_id

    @property
    def artifacts_dir(self) -> Path:
        return self._runtime.artifacts.run_dir(self.run_id)

    def _check(self) -> None:
        if _current_scope.get() != self.scope_id:
            raise RuntimeError("Submit children from a flow/subflow, not from a running leaf task")

    def submit[T](
        self,
        definition: TaskDef[..., T],
        *args: Any,
        options: SubmitOptions | None = None,
        **kwargs: Any,
    ) -> TaskHandle[T]:
        self._check()
        return self._runtime.submit(self.run_id, self.scope_id, definition, args, kwargs, options)

    async def asubmit[T](
        self,
        definition: TaskDef[..., T],
        *args: Any,
        options: SubmitOptions | None = None,
        **kwargs: Any,
    ) -> TaskHandle[T]:
        self._check()
        handle = self._runtime.submit(
            self.run_id,
            self.scope_id,
            definition,
            args,
            kwargs,
            options,
            deferred=True,
        )
        await await_shared(self._runtime.admission(handle.id))
        return handle

    def subflow[**P, T](
        self,
        definition: FlowDef[P, T] | Callable[Concatenate[FlowContext, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> FlowHandle[T]:
        self._check()
        return self._runtime.subflow(self.run_id, self.scope_id, definition, args, kwargs)

    async def all_settled[T](self, handles: Iterable[TaskHandle[T]]) -> list[Outcome[T]]:
        handles = tuple(handles)
        for handle in handles:
            self._runtime.completion(handle.id).observed = True

        async def settle(handle: TaskHandle[T]) -> Outcome[T]:
            try:
                return Outcome(handle.id, value=await handle.result())
            except Exception as exc:
                return Outcome(handle.id, error=exc)

        return list(await asyncio.gather(*(settle(handle) for handle in handles)))

    async def map[T](
        self, definition: TaskDef[..., T], iterable: Iterable[Any], *, max_in_flight: int = 32
    ) -> list[T]:
        """Bound submitted-but-uncollected work, preserving input order in the result."""
        if (
            isinstance(max_in_flight, bool)
            or not isinstance(cast(object, max_in_flight), int)
            or max_in_flight < 1
        ):
            raise ValueError("max_in_flight must be a positive integer")
        iterator = iter(iterable)
        pending: dict[asyncio.Future[T], int] = {}
        results: list[T | None] = []
        exhausted = False
        while pending or not exhausted:
            while len(pending) < max_in_flight and not exhausted:
                try:
                    item = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                handle = await self.asubmit(definition, item)
                self._runtime.completion(handle.id).observed = True
                pending[self._runtime.completion(handle.id).future] = len(results)
                results.append(None)
            if pending:
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for future in done:
                    results[pending.pop(future)] = future.result()
        # Every placeholder has been replaced after all submitted futures finish.
        return cast(list[T], results)

    async def update_metrics(self, **metrics: Any) -> None:
        await self._runtime.command("metrics", self.run_id, payload=metrics)


class EventLoopRuntime(Runtime):
    """Execute flows with local checkpoints and automatic continuation by default.

    Repeating a command with the same flow and explicit inputs opens its unfinished
    run. Successful tasks return their checkpointed values; failed or interrupted
    tasks get a new attempt. Completed runs start fresh. Pass resume=False to
    deliberately start over, or run_key to name an invocation independently of argv.

    Controllers run again to reconstruct dynamic submissions. Keep their order
    deterministic, pass changing data as explicit inputs, and put side effects in
    tasks that tolerate interrupted execution. External submissions to keep_open
    runs must be submitted again by the caller. Results must support the selected
    CheckpointCodec; the default pickle codec requires trusted local run files.
    """

    def __init__(
        self,
        *,
        concurrency: int | None = None,
        scheduler: Scheduler | None = None,
        resources: ResourceAllocator | None = None,
        inputs: InputResolver | None = None,
        dependencies: DependencyPolicy | None = None,
        admission: AdmissionPolicy | None = None,
        retries: RetryPolicy | None = None,
        failures: FailurePolicy | None = None,
        runners: Mapping[str, Runner] | None = None,
        backends: Mapping[str, AgentBackend] | None = None,
        state: StateStore | None = None,
        journal: EventJournal | None = None,
        transport: EventTransport[_Message] | None = None,
        clock: Clock | None = None,
        artifacts: ArtifactStore | None = None,
        checkpoint_codec: CheckpointCodec | None = None,
        resume: bool = True,
        run_key: str | None = None,
    ) -> None:
        if concurrency is not None and resources is not None:
            raise ConfigurationError("Pass concurrency or resources, not both")
        self.scheduler: Scheduler = scheduler if scheduler is not None else PartialOrderScheduler()
        self.resources: ResourceAllocator = (
            resources
            if resources is not None
            else TokenResourceAllocator(
                global_slots=4 if concurrency is None else concurrency,
            )
        )
        self.inputs: InputResolver = inputs if inputs is not None else NestedInputResolver()
        self.dependencies: DependencyPolicy = (
            dependencies if dependencies is not None else AllSuccessful()
        )
        self.admission: AdmissionPolicy = admission if admission is not None else BoundedAdmission()
        self.retries: RetryPolicy = retries if retries is not None else NoRetry()
        self.failures: FailurePolicy = failures if failures is not None else FailScope()
        self.state: StateStore = state if state is not None else MemoryStateStore()
        self.journal: EventJournal = journal if journal is not None else MemoryEventJournal()
        self.transport: EventTransport[_Message] = (
            transport if transport is not None else AsyncioEventTransport()
        )
        self.clock: Clock = clock if clock is not None else MonotonicClock()
        self.artifacts: ArtifactStore = artifacts if artifacts is not None else FileArtifactStore()
        self.checkpoint_codec: CheckpointCodec = (
            checkpoint_codec if checkpoint_codec is not None else PickleCheckpointCodec()
        )
        self.resume: bool = resume
        self.run_key: str | None = run_key
        self._run_metadata: dict[str, dict[str, Any]] = {}
        self._resumed: set[str] = set()
        self._invocations: dict[str, int] = {}
        self._sequences: dict[str, int] = {}
        self.runners: dict[str, Runner] = {
            "python": PythonRunner(),
            "agent": AgentRunner(backends or {}),
        }
        if runners is not None:
            self.runners.update(runners)
        self.scheduler.bind_resources(self.resources)
        self._resource_state = self.resources.initial()
        self._engine: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._records: dict[str, _Record] = {}
        self._handles: dict[str, TaskHandle[Any]] = {}
        self._completions: dict[str, Completion[Any]] = {}
        self._admissions: dict[str, asyncio.Future[None]] = {}
        self._bridge = RuntimeBridge(
            self.state,
            self._command,
            self._submit,
            self._subflow,
            self._handles.__getitem__,
            self.result_details,
            self._completions.__getitem__,
            self._admissions.__getitem__,
            self.artifacts,
        )
        self._runs: dict[str, _Run] = {}
        self._run_handles: dict[str, RunHandle[Any]] = {}
        self._scope_handles: dict[str, FlowHandle[Any]] = {}
        self._scopes: dict[str, _Scope] = {}
        self._dependents: dict[str, dict[str, None]] = {}
        self._dirty: dict[str, None] = {}
        self._pending: dict[str, int] = {}
        self._acks: set[asyncio.Future[None]] = set()
        self._ready_sequence = itertools.count(1)
        self._event_sequence = max((e.sequence for e in self.journal.read()), default=0)
        self._policy_timer: Timer | None = None
        self._policy_generation = 0
        self._aborting = False

    def _ensure_engine(self) -> None:
        loop = asyncio.get_running_loop()
        if self._engine is not None and not self._engine.done():
            if self._loop is not loop:
                raise RuntimeError("A Runtime cannot be driven by two event loops at once")
            return
        self._loop = loop
        self._aborting = False
        self.transport.open()
        self._engine = loop.create_task(self._drive(), name="orchlet-runtime")

    def _post(self, message: _Message) -> None:
        if message.run_id is not None:
            self._pending[message.run_id] = self._pending.get(message.run_id, 0) + 1
        self.transport.send(message)

    def _timer_message(self, message: _Message) -> None:
        if self._engine is None or self._engine.done():
            return
        if message.run_id is not None and self._runs[message.run_id].handle.done:
            return
        self._post(message)

    async def _command(
        self, kind: str, run_id: str | None = None, task_id: str | None = None, payload: Any = None
    ) -> None:
        if self._engine is None or self._engine.done():
            raise RunClosedError("Runtime has no active runs")
        if self._loop is not asyncio.get_running_loop():
            raise RuntimeError("Control commands must use the Runtime's event loop")
        assert self._loop is not None
        ack: asyncio.Future[None] = quiet_future(self._loop.create_future())
        self._acks.add(ack)
        ack.add_done_callback(self._acks.discard)
        self._post(_Message(kind, run_id, task_id, payload, ack))
        return await await_shared(ack)

    def start[T](
        self,
        flow: FlowDef[..., T] | Callable[..., Awaitable[T]],
        *args: Any,
        keep_open: bool = False,
        **kwargs: Any,
    ) -> RunHandle[T]:
        definition: FlowDef[..., T] = (
            cast(FlowDef[..., T], flow)
            if isinstance(flow, FlowDef)
            else FlowDef[..., T](CoroutineFlowController[..., T](flow), flow.__name__)
        )
        controller = definition.controller
        function = (
            controller.function if isinstance(controller, CoroutineFlowController) else controller
        )
        identity = callable_identity(function)
        signature = fingerprint((definition.name, identity, args, kwargs, keep_open))
        key = self.run_key or fingerprint((str(Path.cwd()), sys.argv, identity[:2], args, kwargs))
        invocation = self._invocations.get(key, 0)
        run_id, resumed = self.artifacts.open_run(f"{key}:{invocation}", resume=self.resume)
        try:
            path = self.artifacts.run_dir(run_id) / "run.json"
            metadata = read_json(path) if resumed and path.exists() else {}
            if metadata and (metadata.get("format") != 1 or metadata.get("signature") != signature):
                raise RecoveryError(
                    "The saved flow or its inputs have changed; use resume=False to start a new run"
                )
            now = datetime.now(timezone.utc).isoformat()
            metadata.update(
                {
                    "format": 1,
                    "run_id": run_id,
                    "flow": definition.name,
                    "signature": signature,
                    "inputs": (args, kwargs),
                    "status": "running",
                    "created_at": metadata.get("created_at", now),
                    "updated_at": now,
                    "resumed": resumed,
                    "error": None,
                    "python": list(sys.version_info[:2]),
                }
            )
            atomic_write(path, json_bytes(metadata))
            self._run_metadata[run_id] = metadata
            if resumed:
                self._resumed.add(run_id)
                events = self.artifacts.run_dir(run_id) / "events.jsonl"
                if events.exists():
                    import json

                    for line in events.read_text().splitlines():
                        try:
                            self._event_sequence = max(
                                self._event_sequence, int(json.loads(line)["sequence"])
                            )
                        except ValueError, KeyError:
                            continue  # A killed writer may leave one incomplete event line.
            self._ensure_engine()
        except BaseException:
            self.artifacts.close_run(run_id)
            raise
        self._invocations[key] = invocation + 1
        completion = Completion[T](quiet_future(asyncio.get_running_loop().create_future()))
        self._completions[run_id] = completion
        handle = RunHandle[T](self._bridge, run_id, completion)
        self._run_handles[run_id] = handle
        self._post(
            _Message("start_run", run_id, payload=(handle, definition, args, kwargs, keep_open))
        )
        return handle

    async def arun[**P, T](
        self,
        flow: FlowDef[P, T] | Callable[Concatenate[FlowContext, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        run = self.start(flow, *args, **kwargs)
        try:
            return await run.wait()
        except asyncio.CancelledError:
            if not run.done:
                await run.cancel()
                await asyncio.gather(run.wait(), return_exceptions=True)
            raise

    async def aclose(self) -> None:
        # Include runs whose start command has not been consumed yet.
        for handle in tuple(self._run_handles.values()):
            if not handle.done:
                await handle.cancel()
        await asyncio.gather(
            *(h.wait() for h in self._run_handles.values()), return_exceptions=True
        )
        if self._engine is not None:
            await asyncio.shield(self._engine)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    def _submit[T](
        self,
        run_id: str,
        scope_id: str | None,
        definition: TaskDef[..., T],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        options: SubmitOptions | None = None,
        deferred: bool = False,
    ) -> TaskHandle[T]:
        if not isinstance(cast(object, definition), TaskDef):
            raise TypeError("Submit a @task/@agent definition; use ctx.subflow for flows")
        if self._engine is None or self._engine.done():
            raise RunClosedError("Runtime has no active run")
        if self._loop is not asyncio.get_running_loop():
            raise RuntimeError("Submit on the Runtime's event loop")
        if run_id in self._runs and self._runs[run_id].handle.done:
            raise RunClosedError("Run has completed")
        if scope_id is None and run_id in self._runs and not self._runs[run_id].keep_open:
            raise RunClosedError("External inputs are closed; start with keep_open=True")
        options = options if options is not None else SubmitOptions()
        references = self.inputs.references((args, kwargs))
        controls = tuple(options.after)
        for ref in (*references, *controls):
            if isinstance(ref, TaskHandle):
                if self._handles.get(ref.id) is not ref or ref.run_id != run_id:
                    raise ValueError("Dependencies must belong to this Runtime and run")
                self._completions[ref.id].observed = True
            elif isinstance(cast(object, ref), OutputRef):
                if ref.run_id != run_id or ref.task_id not in self._handles:
                    raise ValueError("Unknown or cross-run input reference")
            else:
                raise TypeError("Control dependencies must be TaskHandles")
        task_id = self._next_id(scope_id or f"{run_id}/external", "task", definition.name)
        completion = Completion[T](quiet_future(asyncio.get_running_loop().create_future()))
        admitted: asyncio.Future[None] = quiet_future(asyncio.get_running_loop().create_future())
        self._completions[task_id] = completion
        self._admissions[task_id] = admitted
        handle = TaskHandle[T](self._bridge, task_id, run_id, completion)
        self._handles[task_id] = handle
        record = _Record(
            handle,
            definition,
            scope_id,
            self.inputs.capture((args, kwargs)),
            tuple(
                dict.fromkeys(r.id if isinstance(r, TaskHandle) else r.task_id for r in references)
            ),
            tuple(dict.fromkeys(r.id for r in controls)),
            options.dependency_policy
            if options.dependency_policy is not None
            else self.dependencies,
            deferred=deferred,
        )
        self._post(_Message("submit", run_id, task_id, record))
        return handle

    def _subflow[T](
        self,
        run_id: str,
        parent: str,
        definition: FlowDef[..., T] | Callable[..., Awaitable[T]],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> FlowHandle[T]:
        definition = (
            cast(FlowDef[..., T], definition)
            if isinstance(definition, FlowDef)
            else FlowDef[..., T](CoroutineFlowController[..., T](definition), definition.__name__)
        )
        scope_id = self._next_id(parent, "flow", definition.name)
        completion = Completion[T](quiet_future(asyncio.get_running_loop().create_future()))
        self._completions[scope_id] = completion
        handle = FlowHandle[T](scope_id, completion)
        self._scope_handles[scope_id] = handle
        self._post(_Message("subflow", run_id, payload=(handle, parent, definition, args, kwargs)))
        return handle

    def _next_id(self, parent: str, kind: str, name: str) -> str:
        key = f"{parent}/{kind}"
        number = self._sequences.get(key, 0) + 1
        self._sequences[key] = number
        return f"{key}-{number}:{quote(name, safe='')}"

    def result_details(self, task_id: str) -> TaskResult[Any] | None:
        record = self._records.get(task_id)
        return record.result if record else None

    async def _check_slot(self, run_id: str, node_id: str) -> None:
        slot = node_id.rsplit(":", 1)[0]
        path = self.artifacts.run_dir(run_id) / "slots" / f"{fingerprint(slot)}.json"
        if run_id in self._resumed and path.exists() and read_json(path)["id"] != node_id:
            raise RecoveryError(
                f"Submission order changed at {slot}; use resume=False for a new run"
            )
        await write_json(path, {"id": node_id})

    def _scope_path(self, run_id: str, scope_id: str) -> Path:
        return self.artifacts.run_dir(run_id) / "scopes" / f"{fingerprint(scope_id)}.json"

    async def _check_scope(
        self,
        run_id: str,
        scope_id: str,
        definition: FlowDef[..., Any],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> None:
        await self._check_slot(run_id, scope_id)
        controller = definition.controller
        function = (
            controller.function if isinstance(controller, CoroutineFlowController) else controller
        )
        signature = fingerprint((definition.name, callable_identity(function), args, kwargs))
        path = self._scope_path(run_id, scope_id)
        if run_id in self._resumed and path.exists():
            if read_json(path).get("signature") != signature:
                raise RecoveryError(
                    f"Flow replay differs at {scope_id}; use resume=False to start a new run"
                )
        await write_json(
            path,
            {
                "scope_id": scope_id,
                "name": definition.name,
                "signature": signature,
                "inputs": (args, kwargs),
            },
        )

    async def _restore_record(self, record: _Record) -> None:
        await self._check_slot(record.handle.run_id, record.handle.id)
        definition = record.definition
        backend = definition.backend
        backend_name = backend if isinstance(backend, str) else None
        runner = self.runners.get(definition.kind)
        if isinstance(backend, str) and isinstance(runner, AgentRunner):
            backend = runner.backends.get(backend)
        backend_config = (
            backend
            if isinstance(backend, str) or backend is None
            else (
                callable_identity(backend),
                {
                    key: getattr(backend, key)
                    for key in ("command", "cwd", "model", "sandbox", "extra_args", "executable")
                    if hasattr(backend, key)
                },
            )
        )
        record.signature = fingerprint(
            (
                definition.name,
                definition.kind,
                callable_identity(definition.function),
                record.inputs,
                record.control_dependencies,
                callable_identity(record.policy),
                backend_config,
                backend_name,
                callable_identity(runner),
                callable_identity(definition.codec),
                callable_identity(definition.validator),
                callable_identity(definition.prompt_builder),
                callable_identity(definition.session),
                definition.context,
                callable_identity(self.checkpoint_codec),
            )
        )
        directory = self.artifacts.task_dir(record.handle.run_id, record.handle.id)
        path = directory / "task.json"
        if record.handle.run_id not in self._resumed:
            return
        if not path.exists():
            if (directory / "attempts").exists():
                raise RecoveryError(f"Task checkpoint is missing: {path}")
            return
        saved = read_json(path)
        if saved.get("format") != 1 or saved.get("signature") != record.signature:
            raise RecoveryError(
                f"Task replay differs at {record.handle.id}; "
                "keep submissions and explicit inputs stable, or use resume=False"
            )
        history: dict[int, dict[str, Any]] = {a["number"]: a for a in saved["attempts"]}
        numbers = [
            int(p.name)
            for p in (directory / "attempts").glob("[0-9]*")
            if p.is_dir() and p.name.isdigit()
        ]
        record.attempt = max(saved["view"]["attempt"], *numbers, 0)
        for number in range(1, record.attempt + 1):
            artifacts = AttemptArtifacts(
                self.artifacts.attempt_dir(record.handle.run_id, record.handle.id, number)
            )
            data: dict[str, Any] | None = history.get(number)
            if data is None:
                request_path = artifacts.directory / "request.json"
                request = read_json(request_path) if request_path.exists() else {}
                data = {
                    "number": number,
                    "started_at": request.get("started_at", saved["started_at"]),
                    "finished_at": self.clock.now(),
                    "error": "Execution interrupted before its result was committed",
                }
                await write_json(artifacts.result_json, data)
            launch = read_json(artifacts.launch_json) if artifacts.launch_json.exists() else {}
            record.attempts.append(
                Attempt(
                    number,
                    data["started_at"],
                    data["finished_at"],
                    data.get("error"),
                    read_text(artifacts.output_txt) if artifacts.output_txt.exists() else None,
                    read_text(artifacts.stdout_log),
                    read_text(artifacts.stderr_log),
                    data.get("session_id"),
                    data.get("exit_code", launch.get("exit_code")),
                    artifacts,
                )
            )
        record.metrics.update(saved["view"]["metrics"])
        if record.attempts:
            record.previous_error = record.attempts[-1].error
            record.previous_output = record.attempts[-1].raw_text
        if saved["view"]["state"] == TaskState.SUCCEEDED:
            record.cached = saved

    async def _reuse_result(self, record: _Record) -> None:
        saved = record.cached
        assert saved is not None
        values: dict[str, Any] = {}
        for key in record.data_dependencies:
            result = self._records[key].result
            assert result is not None
            values[key] = result.value
        resolved = self.inputs.resolve(record.inputs, values)
        if fingerprint(resolved) != saved["resolved_signature"]:
            raise RecoveryError(f"Resolved inputs changed for saved task {record.handle.id}")
        path = self.artifacts.task_dir(record.handle.run_id, record.handle.id) / "value.pickle"
        try:
            payload = await asyncio.to_thread(path.read_bytes)
            if fingerprint(payload) != saved["value_digest"]:
                raise RecoveryError(f"Saved result is damaged: {path}")
            value = self.checkpoint_codec.decode(payload)
        except OSError as exc:
            raise RecoveryError(f"Saved result is missing: {path}") from exc
        last = record.attempts[-1]
        record.result = TaskResult(
            value,
            tuple(record.attempts),
            last.raw_text,
            last.stdout,
            last.stderr,
            last.session_id,
            artifacts=last.artifacts,
            exit_code=last.exit_code,
        )
        record.resolved_signature = saved["resolved_signature"]
        record.value_digest = saved["value_digest"]
        record.cached = None
        await self._finish(record, TaskState.SUCCEEDED)
        await self._emit(
            "task_restored", record.handle.run_id, record.handle.id, name=record.definition.name
        )

    async def _finish_run_storage(self, run_id: str, error: BaseException | None) -> None:
        metadata = self._run_metadata[run_id]
        metadata.update(
            {
                "status": "succeeded"
                if error is None
                else "interrupted"
                if isinstance(error, TaskCancelled)
                else "failed",
                "error": f"{type(error).__name__}: {error}" if error is not None else None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "metrics": self._runs[run_id].metrics if run_id in self._runs else {},
            }
        )
        try:
            await write_json(self.artifacts.run_dir(run_id) / "run.json", metadata)
        finally:
            self.artifacts.close_run(run_id)

    async def _persist(self, record: _Record | None = None) -> None:
        if not self._aborting:
            snapshot = self.state.snapshot()
            changes = {record.handle.id: record.view()} if record is not None else {}
            await self.state.commit(snapshot.revision, changes)
            if record is not None and record.cached is None:
                await write_json(
                    self.artifacts.task_dir(record.handle.run_id, record.handle.id) / "task.json",
                    {
                        "format": 1,
                        "view": record.view(),
                        "signature": record.signature,
                        "inputs": record.inputs,
                        "control_dependencies": record.control_dependencies,
                        "attempts": [attempt_metadata(a) for a in record.attempts],
                        "started_at": record.started_at,
                        "resolved_signature": record.resolved_signature,
                        "value_digest": record.value_digest,
                        "result": result_metadata(record.result)
                        if record.result is not None
                        else None,
                        "error": str(record.error) if record.error is not None else None,
                    },
                )

    async def _emit(
        self, kind: str, run_id: str | None = None, task_id: str | None = None, **data: Any
    ) -> None:
        if self._aborting:
            return
        self._event_sequence += 1
        event = RuntimeEvent(self._event_sequence, self.clock.now(), kind, run_id, task_id, data)
        await self.journal.append(event)
        if run_id is not None:
            await self.artifacts.append_event(run_id, event)
        self.scheduler.observe(event)
        log_event(event)

    def _spawn_scope(
        self,
        run_id: str,
        handle: FlowHandle[Any],
        parent: str | None,
        definition: FlowDef[..., Any],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> _Scope:
        scope = _Scope(handle.id, run_id, handle, parent)
        self._scope_handles[scope.id] = handle
        self._scopes[scope.id] = scope
        if parent:
            self._scopes[parent].children.append(scope.id)

        async def drive_flow() -> None:
            token = _current_scope.set(scope.id)
            value, error = None, None
            try:
                context = FlowContext(self._bridge, run_id, scope.id)
                value = await definition.controller.run(context, args, kwargs)
            except BaseException as exc:
                error = (
                    TaskCancelled("Flow cancelled")
                    if isinstance(exc, asyncio.CancelledError)
                    else exc
                )
            finally:
                _current_scope.reset(token)
            self._post(_Message("flow_done", run_id, payload=(scope.id, value, error)))

        assert self._loop is not None
        scope.worker = self._loop.create_task(drive_flow(), name=f"orchlet:{definition.name}")

        def cancelled_before_start(worker: asyncio.Task[None]) -> None:
            if worker.cancelled():
                self._post(
                    _Message(
                        "flow_done",
                        run_id,
                        payload=(
                            scope.id,
                            None,
                            TaskCancelled("Flow cancelled before starting"),
                        ),
                    )
                )

        scope.worker.add_done_callback(cancelled_before_start)
        return scope

    async def _drive(self) -> None:
        try:
            while True:
                first = await self.transport.receive()
                messages = [first, *self.transport.drain(255)]
                changed = False
                for message in messages:
                    try:
                        changed = await self._apply(message) or changed
                        if message.ack is not None and not message.ack.done():
                            message.ack.set_result(None)
                    except (RunClosedError, ValueError, KeyError) as exc:
                        if message.ack is None:
                            raise
                        message.ack.set_exception(exc)
                    finally:
                        if message.run_id is not None:
                            self._pending[message.run_id] -= 1
                await self._admit_deferred()
                await self._refresh_dependencies()
                await self._settle_scopes()
                if changed:
                    await self._schedule(first.kind)
                await self._settle_runs()
                if (
                    self._runs
                    and all(run.handle.done for run in self._runs.values())
                    and self.transport.empty()
                ):
                    break
                # Allow workers and flow continuations to progress under a busy inbox.
                await asyncio.sleep(0)
        except BaseException as exc:
            await self._abort(exc)
        finally:
            if self._policy_timer is not None:
                self._policy_timer.cancel()
                self._policy_timer = None

    async def _apply(self, message: _Message) -> bool:
        kind, run_id, task_id, payload = (
            message.kind,
            message.run_id,
            message.task_id,
            message.payload,
        )
        if kind == "policy_wake":
            return payload == self._policy_generation
        assert run_id is not None
        if kind == "start_run":
            handle, definition, args, kwargs, keep_open = payload
            completion = Completion[Any](quiet_future(asyncio.get_running_loop().create_future()))
            root = FlowHandle[Any](f"{run_id}/root", completion)
            self._completions[root.id] = completion
            self._runs[run_id] = _Run(
                handle,
                root.id,
                keep_open,
                metrics=dict(self._run_metadata[run_id].get("metrics", {})),
            )
            await self._check_scope(run_id, root.id, definition, args, kwargs)
            self._spawn_scope(run_id, root, None, definition, args, kwargs)
            await self._emit(
                "run_resumed" if run_id in self._resumed else "run_started",
                run_id,
                directory=str(self.artifacts.run_dir(run_id)),
            )
        elif kind == "submit":
            assert task_id is not None
            record = payload
            self._records[task_id] = record
            await self._restore_record(record)
            record.submitted_at = self.clock.now()
            run = self._runs[run_id]
            if record.scope_id is None:
                closed = not run.keep_open
                if not closed:
                    run.external.append(task_id)
            else:
                scope = self._scopes[record.scope_id]
                scope.tasks.append(task_id)
                closed = scope.flow_done
            if closed or run.cancelled or run.handle.done:
                await self._finish(
                    record, TaskState.CANCELLED, error=RunClosedError("Submission scope is closed")
                )
            else:
                await self._admit(record)
        elif kind == "subflow":
            handle, parent, definition, args, kwargs = payload
            scope = self._scopes[parent]
            if scope.flow_done or self._runs[run_id].cancelled:
                self._completions[handle.id].future.set_exception(
                    RunClosedError("Parent flow is closed")
                )
            else:
                await self._check_scope(run_id, handle.id, definition, args, kwargs)
                self._spawn_scope(run_id, handle, parent, definition, args, kwargs)
        elif kind == "flow_done":
            scope_id, value, error = payload
            scope = self._scopes[scope_id]
            scope.flow_done, scope.value, scope.error = True, value, error
            await write_json(
                self._scope_path(run_id, scope_id).with_suffix(".result.json"),
                {"value": value, "error": str(error) if error is not None else None},
            )
            if error is not None:
                await self._cancel_scope_children(scope)
        elif kind == "complete":
            assert task_id is not None
            await self._complete(self._records[task_id], *payload)
        elif kind == "retry":
            assert task_id is not None
            record = self._records[task_id]
            if record.state == TaskState.RETRY_WAIT and record.attempt == payload:
                record.timer = None
                await self._ready(record)
        elif kind == "timeout":
            assert task_id is not None
            record = self._records[task_id]
            if record.state == TaskState.RUNNING and record.attempt == payload:
                record.timed_out = True
                record.state = TaskState.CANCELLING
                record.token.request()
                await self._persist(record)
                await self._emit("task_timeout", run_id, task_id, attempt=record.attempt)
        elif kind == "cancel_task":
            assert task_id is not None
            record = self._records[task_id]
            if record.handle.run_id != run_id:
                raise ValueError("Task belongs to another run")
            await self._cancel_record(record)
        elif kind == "cancel_run":
            run = self._runs[run_id]
            if not run.handle.done:
                run.cancelled, run.keep_open = True, False
                root = self._scopes[run.root]
                if not root.flow_done:
                    assert root.worker is not None
                    root.worker.cancel()
                await self._cancel_scope_children(root)
                for node_id in run.external:
                    await self._cancel_record(self._records[node_id])
        elif kind == "close_inputs":
            self._runs[run_id].keep_open = False
        elif kind == "metrics":
            run = self._runs[run_id]
            if run.handle.done:
                raise RunClosedError("Run has completed")
            if task_id is None:
                run.metrics.update(payload)
                self._run_metadata[run_id]["metrics"] = run.metrics
                await write_json(
                    self.artifacts.run_dir(run_id) / "run.json", self._run_metadata[run_id]
                )
                await self._persist()
            else:
                record = self._records[task_id]
                if record.state.terminal:
                    return False
                record.metrics.update(payload)
                await self._persist(record)
            await self._emit("metrics_updated", run_id, task_id, metrics=payload)
        elif kind == "runner_event":
            assert task_id is not None
            attempt, event_kind, data = payload
            record = self._records[task_id]
            if record.attempt != attempt or record.state.terminal:
                return False
            if event_kind == "metrics":
                record.metrics.update(data)
                await self._persist(record)
            await self._emit(event_kind, run_id, task_id, **{**data, "attempt": attempt})
            return event_kind == "metrics"
        else:
            raise RuntimeError(f"Unknown runtime message: {kind}")
        return True

    async def _admit(self, record: _Record) -> None:
        if record.definition.kind not in self.runners:
            await self._finish(
                record,
                TaskState.FAILED,
                error=ConfigurationError(
                    f"Unregistered runner: {record.definition.kind}",
                ),
            )
            return
        try:
            possible = self.resources.plan([record.view()], self.resources.initial())
        except Exception as exc:
            await self._finish(record, TaskState.FAILED, error=exc)
            return
        if possible is None:
            await self._finish(
                record,
                TaskState.FAILED,
                error=ConfigurationError(
                    "Task resource requirements exceed configured capacities",
                ),
            )
            return
        outstanding = sum(r.admitted and not r.state.terminal for r in self._records.values())
        if not self.admission.admit(record.view(), outstanding):
            if record.deferred:
                await self._persist(record)
            else:
                await self._finish(
                    record, TaskState.FAILED, error=AdmissionError("Pending task limit reached")
                )
            return
        record.admitted = True
        record.state = TaskState.WAITING
        for dependency in (*record.data_dependencies, *record.control_dependencies):
            self._dependents.setdefault(dependency, {}).setdefault(record.handle.id, None)
        self._dirty.setdefault(record.handle.id, None)
        self._admissions[record.handle.id].set_result(None)
        await self._persist(record)
        await self._emit("task_submitted", record.handle.run_id, record.handle.id)

    async def _admit_deferred(self) -> None:
        for record in tuple(self._records.values()):
            if record.state == TaskState.SUBMITTED and record.deferred:
                await self._admit(record)

    async def _refresh_dependencies(self) -> None:
        while self._dirty:
            node_id = next(iter(self._dirty))
            del self._dirty[node_id]
            record = self._records[node_id]
            if record.state != TaskState.WAITING:
                continue
            data = [self._records[d].state for d in record.data_dependencies]
            if any(s.terminal and s != TaskState.SUCCEEDED for s in data):
                await self._finish(
                    record, TaskState.SKIPPED, error=DependencyFailed("Required input failed")
                )
                continue
            if not all(s == TaskState.SUCCEEDED for s in data):
                continue
            gate = record.policy.evaluate(
                [self._records[d].state for d in record.control_dependencies]
            )
            if gate == Gate.IMPOSSIBLE:
                await self._finish(
                    record, TaskState.SKIPPED, error=DependencyFailed("Control dependencies failed")
                )
            elif gate == Gate.PASS:
                await self._ready(record)
            elif gate != Gate.WAIT:
                raise ConfigurationError("DependencyPolicy must return Gate")

    async def _ready(self, record: _Record) -> None:
        if record.cached is not None:
            await self._reuse_result(record)
            return
        record.state = TaskState.READY
        record.ready_at = self.clock.now()
        record.sequence = next(self._ready_sequence)
        await self._persist(record)
        await self._emit("task_ready", record.handle.run_id, record.handle.id)

    async def _schedule(self, trigger: str) -> None:
        views = tuple(
            replace(
                record.view(),
                waiting_dependents=sum(
                    self._records[node_id].state == TaskState.WAITING
                    for node_id in self._dependents.get(record.handle.id, {})
                ),
            )
            for record in self._records.values()
            if not record.state.terminal
        )
        snapshot = ScheduleSnapshot(
            self.state.snapshot().revision,
            self.clock.now(),
            tuple(t for t in views if t.state == TaskState.READY),
            tuple(t for t in views if t.state in (TaskState.RUNNING, TaskState.CANCELLING)),
            self._resource_state,
            {run_id: run.metrics for run_id, run in self._runs.items() if not run.handle.done},
            trigger,
        )
        decision = self.scheduler.schedule(snapshot)
        if (
            not isinstance(cast(object, decision), ScheduleDecision)
            or decision.revision != snapshot.revision
        ):
            raise SchedulingError("Scheduler returned an invalid or stale decision")
        ids = [start.task_id for start in decision.starts]
        if len(set(ids)) != len(ids):
            raise SchedulingError("Scheduler selected a task twice")
        ready = {task.id: task for task in snapshot.ready}
        if any(node_id not in ready for node_id in ids):
            raise SchedulingError("Scheduler selected a task that is not READY")
        if decision.wake_at is not None and (
            not math.isfinite(decision.wake_at) or decision.wake_at <= snapshot.now
        ):
            raise SchedulingError("wake_at must be a future monotonic deadline")
        plan = self.resources.plan([ready[node_id] for node_id in ids], snapshot.resources)
        if plan is None:
            raise SchedulingError("Scheduler overcommitted available resources")
        if set(plan.reservations) != set(ids):
            raise SchedulingError("Allocator returned the wrong reservation set")
        for key, available in plan.remaining.available.items():
            if not math.isfinite(available) or not 0 <= available <= plan.remaining.capacity[key]:
                raise SchedulingError("Allocator returned invalid remaining capacity")
        self._resource_state = plan.remaining
        # Reserve the whole batch before yielding to any Runner or journal I/O.
        for node_id in ids:
            self._records[node_id].leases = dict(plan.reservations[node_id])
        for start in decision.starts:
            await self._launch(self._records[start.task_id], start)
        self._policy_generation += 1
        if self._policy_timer is not None:
            self._policy_timer.cancel()
            self._policy_timer = None
        if decision.wake_at is not None:
            generation = self._policy_generation
            self._policy_timer = self.clock.schedule_at(
                decision.wake_at,
                lambda: self._timer_message(_Message("policy_wake", payload=generation)),
            )
        if snapshot.ready and not ids and decision.wake_at is None and not snapshot.running:
            await self._emit("scheduler_waiting", reason="Policy selected no tasks and no wakeup")

    async def _launch(self, record: _Record, start: Start) -> None:
        record.state = TaskState.RUNNING
        record.attempt += 1
        record.started_at = self.clock.now()
        record.timed_out = False
        record.token = CancellationToken()
        values: dict[str, Any] = {}
        for dependency in record.data_dependencies:
            details = self._records[dependency].result
            assert details is not None
            values[dependency] = details.value
        args, kwargs = self.inputs.resolve(record.inputs, values)
        record.resolved_signature = fingerprint((args, kwargs))
        artifacts = AttemptArtifacts(
            self.artifacts.attempt_dir(record.handle.run_id, record.handle.id, record.attempt)
        )
        await write_json(
            artifacts.directory / "request.json",
            {
                "task_id": record.handle.id,
                "attempt": record.attempt,
                "args": args,
                "kwargs": kwargs,
                "started_at": record.started_at,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        request = ExecutionRequest(
            record.handle.id,
            record.definition,
            args,
            kwargs,
            AttemptContext(
                record.handle.id, record.attempt, record.previous_error, record.previous_output
            ),
            self.clock,
            artifacts,
        )
        await self._persist(record)
        await self._emit(
            "task_started",
            record.handle.run_id,
            record.handle.id,
            attempt=record.attempt,
            reason=start.reason,
            score=start.score,
        )
        attempt = record.attempt
        loop = self._loop
        assert loop is not None

        def emit(kind: str, data: Mapping[str, Any]) -> None:
            message = _Message(
                "runner_event", record.handle.run_id, record.handle.id, (attempt, kind, data)
            )
            # Python tasks can report from a worker thread.
            loop.call_soon_threadsafe(self._timer_message, message)

        async def execute() -> None:
            result, error = None, None
            context_token = _current_scope.set(None)
            try:
                if record.token.requested:
                    raise TaskCancelled("Cancelled before runner started")
                result = await self.runners[record.definition.kind].execute(
                    request, emit, record.token
                )
            except BaseException as exc:
                error = exc
            finally:
                _current_scope.reset(context_token)
            self._post(
                _Message(
                    "complete", record.handle.run_id, record.handle.id, (attempt, result, error)
                )
            )

        record.worker = loop.create_task(execute(), name=f"orchlet:{record.definition.name}")
        if record.definition.timeout is not None:
            record.timer = self.clock.schedule_at(
                self.clock.now() + record.definition.timeout,
                lambda: self._timer_message(
                    _Message("timeout", record.handle.run_id, record.handle.id, attempt)
                ),
            )

    def _release(self, record: _Record) -> None:
        if record.timer is not None:
            record.timer.cancel()
            record.timer = None
        available = dict(self._resource_state.available)
        for key, amount in record.leases.items():
            available[key] += amount
            capacity = self._resource_state.capacity[key]
            if available[key] > capacity + 1e-9:
                raise SchedulingError("Resource released more than once")
            available[key] = min(available[key], capacity)
        record.leases.clear()
        self._resource_state = ResourceSnapshot(self._resource_state.capacity, available)

    async def _complete(
        self,
        record: _Record,
        attempt: int,
        result: ExecutionResult[Any] | None,
        error: BaseException | None,
    ) -> None:
        if record.attempt != attempt or record.state not in (
            TaskState.RUNNING,
            TaskState.CANCELLING,
        ):
            return
        record.worker = None
        self._release(record)
        diagnostic = result if isinstance(result, ExecutionResult) else error
        if record.cancel_requested:
            error = TaskCancelled("Task cancelled")
        elif record.timed_out:
            error = TimeoutError(f"Task exceeded {record.definition.timeout}s timeout")
        elif isinstance(error, asyncio.CancelledError):
            error = TaskCancelled("Runner cancelled")
        if error is None and not isinstance(result, ExecutionResult):
            error = ConfigurationError("Runner.execute() must return ExecutionResult")
            result = None
        artifacts = AttemptArtifacts(
            self.artifacts.attempt_dir(record.handle.run_id, record.handle.id, attempt)
        )
        raw_text: str | None = getattr(diagnostic, "raw_text", None)
        if raw_text is not None and not artifacts.output_txt.exists():
            await write_text(artifacts.output_txt, raw_text)
        if artifacts.output_txt.exists():
            raw_text = read_text(artifacts.output_txt)
        for path, name in ((artifacts.stdout_log, "stdout"), (artifacts.stderr_log, "stderr")):
            if not path.exists():
                await write_text(path, getattr(diagnostic, name, ""))
        stdout, stderr = read_text(artifacts.stdout_log), read_text(artifacts.stderr_log)
        session_id: str | None = getattr(diagnostic, "session_id", None)
        exit_code: int | None = getattr(diagnostic, "exit_code", None)
        if exit_code is None and artifacts.launch_json.exists():
            exit_code = read_json(artifacts.launch_json).get("exit_code")
        if session_id is None and record.definition.kind == "agent":
            import json

            for line in stdout.splitlines():
                try:
                    event: Any = json.loads(line)
                    if (
                        isinstance(event, dict)
                        and cast(dict[str, Any], event).get("type") == "thread.started"
                    ):
                        session_id = cast(dict[str, Any], event).get("thread_id")
                except ValueError:
                    continue
        if error is None:
            assert result is not None
            try:
                payload = self.checkpoint_codec.encode(result.value)
            except Exception as exc:
                error = exc
            else:
                await asyncio.to_thread(
                    atomic_write,
                    self.artifacts.task_dir(record.handle.run_id, record.handle.id)
                    / "value.pickle",
                    payload,
                )
                record.value_digest = fingerprint(payload)
        record.attempts.append(
            Attempt(
                attempt,
                record.started_at,
                self.clock.now(),
                f"{type(error).__name__}: {error}" if error else None,
                raw_text,
                stdout,
                stderr,
                session_id,
                exit_code,
                artifacts,
            )
        )
        await write_json(
            artifacts.result_json,
            {
                **attempt_metadata(record.attempts[-1]),
                "value": result.value if error is None and result is not None else None,
                "traceback": "".join(traceback.format_exception(error))
                if error is not None
                else None,
            },
        )
        if record.cancel_requested:
            await self._finish(record, TaskState.CANCELLED, error=error)
        elif error is None:
            assert result is not None
            record.result = TaskResult(
                result.value,
                tuple(record.attempts),
                raw_text,
                stdout,
                stderr,
                session_id,
                artifacts=artifacts,
                exit_code=exit_code,
            )
            await self._finish(record, TaskState.SUCCEEDED)
        else:
            retry = record.definition.retry if record.definition.retry is not None else self.retries
            decision = retry.decide(error, tuple(record.attempts))
            if decision.retry:
                if not math.isfinite(decision.delay) or decision.delay < 0:
                    raise ConfigurationError("RetryPolicy returned an invalid delay")
                record.previous_error = f"{type(error).__name__}: {error}"
                record.previous_output = raw_text
                record.state = TaskState.RETRY_WAIT
                await self._persist(record)
                await self._emit(
                    "task_retry",
                    record.handle.run_id,
                    record.handle.id,
                    attempt=attempt,
                    delay=decision.delay,
                    error=record.previous_error,
                )
                record.timer = self.clock.schedule_at(
                    self.clock.now() + decision.delay,
                    lambda: self._timer_message(
                        _Message("retry", record.handle.run_id, record.handle.id, attempt)
                    ),
                )
            else:
                await self._finish(
                    record, TaskState.FAILED, error=TaskFailed(record.handle.id, error)
                )

    async def _finish(
        self, record: _Record, state: TaskState, error: BaseException | None = None
    ) -> None:
        record.state, record.error = state, error
        if error is not None:
            last = record.attempts[-1] if record.attempts else None
            record.result = TaskResult(
                None,
                tuple(record.attempts),
                last.raw_text if last is not None else None,
                last.stdout if last is not None else "",
                last.stderr if last is not None else "",
                last.session_id if last is not None else None,
                error=error,
                artifacts=last.artifacts if last is not None else None,
                exit_code=last.exit_code if last is not None else None,
            )
        if record.timer is not None:
            record.timer.cancel()
            record.timer = None
        await self._persist(record)
        await self._emit(
            "task_finished",
            record.handle.run_id,
            record.handle.id,
            state=state.value,
            error=str(error) if error else None,
        )
        future = self._completions[record.handle.id].future
        if not future.done():
            if error is None:
                assert record.result is not None
                future.set_result(record.result.value)
            else:
                future.set_exception(error)
        if not self._admissions[record.handle.id].done():
            self._admissions[record.handle.id].set_exception(error or TaskCancelled("Not admitted"))
        self._dirty.update(self._dependents.get(record.handle.id, {}))

    async def _cancel_record(self, record: _Record) -> None:
        if record.state.terminal:
            return
        record.cancel_requested = True
        if record.worker is not None:
            record.state = TaskState.CANCELLING
            record.token.request()
            await self._persist(record)
        else:
            await self._finish(
                record, TaskState.CANCELLED, error=TaskCancelled("Task cancelled before execution")
            )

    async def _cancel_scope_children(self, scope: _Scope) -> None:
        for node_id in scope.tasks:
            await self._cancel_record(self._records[node_id])
        for scope_id in scope.children:
            child = self._scopes[scope_id]
            if not child.flow_done:
                assert child.worker is not None
                child.worker.cancel()
            await self._cancel_scope_children(child)

    async def _settle_scopes(self) -> None:
        changed = True
        while changed:
            changed = False
            for scope in tuple(self._scopes.values()):
                if scope.settled or not scope.flow_done:
                    continue
                tasks = [self._records[node_id] for node_id in scope.tasks]
                children = [self._scopes[scope_id] for scope_id in scope.children]
                errors = [
                    r.error
                    for r in tasks
                    if r.error is not None and not self._completions[r.handle.id].observed
                ]
                errors += [
                    c.error
                    for c in children
                    if c.error is not None and not self._completions[c.handle.id].observed
                ]
                if scope.error is None and self.failures.fail_scope(errors):
                    scope.error = errors[0]
                    await self._cancel_scope_children(scope)
                if any(not r.state.terminal for r in tasks) or any(not s.settled for s in children):
                    continue
                scope.settled = True
                if scope.error is None:
                    self._completions[scope.handle.id].future.set_result(scope.value)
                else:
                    self._completions[scope.handle.id].future.set_exception(scope.error)
                changed = True

    async def _settle_runs(self) -> None:
        for run_id, run in self._runs.items():
            if run.handle.done or run.keep_open or self._pending.get(run_id, 0):
                continue
            root = self._scopes[run.root]
            external = [self._records[node_id] for node_id in run.external]
            if not root.settled or any(not r.state.terminal for r in external):
                continue
            error = TaskCancelled("Run cancelled") if run.cancelled else root.error
            errors = [
                r.error
                for r in external
                if r.error is not None and not self._completions[r.handle.id].observed
            ]
            if error is None and self.failures.fail_scope(errors):
                error = errors[0]
            await self._emit("run_finished", run_id, error=str(error) if error else None)
            await self._finish_run_storage(run_id, error)
            if error is None:
                self._completions[run.handle.id].future.set_result(root.value)
            else:
                self._completions[run.handle.id].future.set_exception(error)

    async def _abort(self, error: BaseException) -> None:
        """Fail all affected promises and await runner shutdown even if a plugin fails."""
        self._aborting = True
        if isinstance(error, asyncio.CancelledError):
            error = TaskCancelled("Runtime stopped")
        log_runtime_error(error)
        workers: list[asyncio.Task[None]] = []
        for scope in self._scopes.values():
            if scope.worker is not None and not scope.worker.done():
                scope.worker.cancel()
                workers.append(scope.worker)
        for record in self._records.values():
            if record.timer is not None:
                record.timer.cancel()
            if record.worker is not None and not record.worker.done():
                record.token.request()
                workers.append(record.worker)
        await asyncio.gather(*workers, return_exceptions=True)
        for record in self._records.values():
            if record.leases:
                self._release(record)
            if not record.state.terminal:
                record.state, record.error = TaskState.FAILED, error
        for handle in self._handles.values():
            for future in (self._completions[handle.id].future, self._admissions[handle.id]):
                if not future.done():
                    future.set_exception(error)
        try:
            snapshot = self.state.snapshot()
            await self.state.commit(
                snapshot.revision, {key: rec.view() for key, rec in self._records.items()}
            )
        except Exception:
            pass  # The original plugin error remains the run's reported failure.
        for handle in self._scope_handles.values():
            if not self._completions[handle.id].future.done():
                self._completions[handle.id].future.set_exception(error)
        for handle in self._run_handles.values():
            if not handle.done:
                try:
                    await self._finish_run_storage(handle.id, error)
                except Exception:
                    self.artifacts.close_run(handle.id)
                self._completions[handle.id].future.set_exception(error)
        for ack in tuple(self._acks):
            if not ack.done():
                ack.set_exception(error)
        self.transport.drain(10**9)
        self._pending.clear()
