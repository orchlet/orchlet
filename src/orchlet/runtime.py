"""Single-writer, event-driven runtime. No worker waits on a dispatch semaphore."""

from __future__ import annotations

import asyncio
import contextvars
import itertools
import math
import uuid
from dataclasses import dataclass, field, replace
from typing import Any

from .clocks import MonotonicClock
from .contracts import Runtime
from .definitions import FlowDef, TaskDef, flow as make_flow
from .errors import (
    AdmissionError,
    ConfigurationError,
    DependencyFailed,
    RunClosedError,
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
    AttemptContext,
    ExecutionRequest,
    ExecutionResult,
    Gate,
    Outcome,
    ResourceSnapshot,
    RuntimeEvent,
    ScheduleDecision,
    ScheduleSnapshot,
    TaskResult,
    TaskState,
    TaskView,
)
from .policies import AllSuccessful, BoundedAdmission, FailScope, NoRetry
from .resources import TokenResourceAllocator
from .runners import AgentRunner, CancellationToken, PythonRunner
from .schedulers import PartialOrderScheduler
from .state import MemoryStateStore

_current_scope = contextvars.ContextVar("orchlet_scope", default=None)


@dataclass(frozen=True)
class SubmitOptions:
    after: tuple[TaskHandle, ...] = ()
    dependency_policy: Any = None


@dataclass
class _Message:
    kind: str
    run_id: str | None = None
    task_id: str | None = None
    payload: Any = None
    ack: Any = None


@dataclass
class _Record:
    handle: TaskHandle
    definition: TaskDef
    scope_id: str | None
    inputs: Any
    data_dependencies: tuple[str, ...]
    control_dependencies: tuple[str, ...]
    policy: Any
    deferred: bool = False
    admitted: bool = False
    state: TaskState = TaskState.SUBMITTED
    submitted_at: float = 0.0
    ready_at: float | None = None
    sequence: int = 0
    metrics: dict = field(default_factory=dict)
    attempts: list = field(default_factory=list)
    attempt: int = 0
    started_at: float = 0.0
    previous_error: str | None = None
    previous_output: str | None = None
    worker: Any = None
    token: Any = None
    timer: Any = None
    leases: dict = field(default_factory=dict)
    cancel_requested: bool = False
    timed_out: bool = False
    result: TaskResult | None = None
    error: BaseException | None = None

    def view(self):
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
    handle: FlowHandle
    parent: str | None = None
    tasks: list[str] = field(default_factory=list)
    children: list[str] = field(default_factory=list)
    worker: Any = None
    flow_done: bool = False
    settled: bool = False
    value: Any = None
    error: BaseException | None = None


@dataclass
class _Run:
    handle: RunHandle
    root: str
    keep_open: bool
    external: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    cancelled: bool = False


class FlowContext:
    def __init__(self, runtime, run_id, scope_id):
        self._runtime, self.run_id, self.scope_id = runtime, run_id, scope_id

    def _check(self):
        if _current_scope.get() != self.scope_id:
            raise RuntimeError("Submit children from a flow/subflow, not from a running leaf task")

    def submit(self, definition, *args, options=None, **kwargs):
        self._check()
        return self._runtime._submit(self.run_id, self.scope_id, definition, args, kwargs, options)

    async def asubmit(self, definition, *args, options=None, **kwargs):
        self._check()
        handle = self._runtime._submit(
            self.run_id,
            self.scope_id,
            definition,
            args,
            kwargs,
            options,
            deferred=True,
        )
        await await_shared(handle._admitted)
        return handle

    def subflow(self, definition, *args, **kwargs):
        self._check()
        return self._runtime._subflow(self.run_id, self.scope_id, definition, args, kwargs)

    async def all_settled(self, handles):
        handles = tuple(handles)
        for handle in handles:
            handle._observed = True

        async def settle(handle):
            try:
                return Outcome(handle.id, value=await handle.result())
            except Exception as exc:
                return Outcome(handle.id, error=exc)

        return list(await asyncio.gather(*(settle(handle) for handle in handles)))

    async def map(self, definition, iterable, *, max_in_flight=32):
        """Bound submitted-but-uncollected work, preserving input order in the result."""
        if (
            isinstance(max_in_flight, bool)
            or not isinstance(max_in_flight, int)
            or max_in_flight < 1
        ):
            raise ValueError("max_in_flight must be a positive integer")
        iterator = iter(iterable)
        pending, results = {}, []
        exhausted = False
        while pending or not exhausted:
            while len(pending) < max_in_flight and not exhausted:
                try:
                    item = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                handle = await self.asubmit(definition, item)
                handle._observed = True
                pending[handle._future] = len(results)
                results.append(None)
            if pending:
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for future in done:
                    results[pending.pop(future)] = future.result()
        return results

    async def update_metrics(self, **metrics):
        await self._runtime._command("metrics", self.run_id, payload=metrics)


class EventLoopRuntime(Runtime):
    def __init__(
        self,
        *,
        concurrency=None,
        scheduler=None,
        resources=None,
        inputs=None,
        dependencies=None,
        admission=None,
        retries=None,
        failures=None,
        runners=None,
        backends=None,
        state=None,
        journal=None,
        transport=None,
        clock=None,
    ):
        if concurrency is not None and resources is not None:
            raise ConfigurationError("Pass concurrency or resources, not both")
        self.scheduler = scheduler if scheduler is not None else PartialOrderScheduler()
        self.resources = (
            resources
            if resources is not None
            else TokenResourceAllocator(
                global_slots=4 if concurrency is None else concurrency,
            )
        )
        self.inputs = inputs if inputs is not None else NestedInputResolver()
        self.dependencies = dependencies if dependencies is not None else AllSuccessful()
        self.admission = admission if admission is not None else BoundedAdmission()
        self.retries = retries if retries is not None else NoRetry()
        self.failures = failures if failures is not None else FailScope()
        self.state = state if state is not None else MemoryStateStore()
        self.journal = journal if journal is not None else MemoryEventJournal()
        self.transport = transport if transport is not None else AsyncioEventTransport()
        self.clock = clock if clock is not None else MonotonicClock()
        self.runners = {"python": PythonRunner(), "agent": AgentRunner(backends or {})}
        if runners is not None:
            self.runners.update(runners)
        self.scheduler.bind_resources(self.resources)
        self._resource_state = self.resources.initial()
        self._engine = None
        self._loop = None
        self._records = {}
        self._handles = {}
        self._runs = {}
        self._run_handles = {}
        self._scope_handles = {}
        self._scopes = {}
        self._dependents = {}
        self._dirty = {}
        self._pending = {}
        self._acks = set()
        self._sequence = itertools.count(1)
        self._ready_sequence = itertools.count(1)
        self._event_sequence = max((e.sequence for e in self.journal.read()), default=0)
        self._policy_timer = None
        self._policy_generation = 0
        self._aborting = False

    def _ensure_engine(self):
        loop = asyncio.get_running_loop()
        if self._engine is not None and not self._engine.done():
            if self._loop is not loop:
                raise RuntimeError("A Runtime cannot be driven by two event loops at once")
            return
        self._loop = loop
        self._aborting = False
        self.transport.open()
        self._engine = loop.create_task(self._drive(), name="orchlet-runtime")

    def _post(self, message):
        if message.run_id is not None:
            self._pending[message.run_id] = self._pending.get(message.run_id, 0) + 1
        self.transport.send(message)

    def _timer_message(self, message):
        if self._engine is None or self._engine.done():
            return
        if message.run_id is not None and self._runs[message.run_id].handle.done:
            return
        self._post(message)

    async def _command(self, kind, run_id=None, task_id=None, payload=None):
        if self._engine is None or self._engine.done():
            raise RunClosedError("Runtime has no active runs")
        if self._loop is not asyncio.get_running_loop():
            raise RuntimeError("Control commands must use the Runtime's event loop")
        ack = quiet_future(self._loop.create_future())
        self._acks.add(ack)
        ack.add_done_callback(self._acks.discard)
        self._post(_Message(kind, run_id, task_id, payload, ack))
        return await await_shared(ack)

    def start(self, flow, *args, keep_open=False, **kwargs):
        definition = flow if isinstance(flow, FlowDef) else make_flow(flow)
        self._ensure_engine()
        run_id = uuid.uuid4().hex[:12]
        handle = RunHandle(self, run_id)
        self._run_handles[run_id] = handle
        self._post(
            _Message("start_run", run_id, payload=(handle, definition, args, kwargs, keep_open))
        )
        return handle

    async def arun(self, flow, *args, **kwargs):
        run = self.start(flow, *args, **kwargs)
        try:
            return await run.wait()
        except asyncio.CancelledError:
            if not run.done:
                await run.cancel()
                await asyncio.gather(run.wait(), return_exceptions=True)
            raise

    async def aclose(self):
        # Include runs whose start command has not been consumed yet.
        for handle in tuple(self._run_handles.values()):
            if not handle.done:
                await handle.cancel()
        await asyncio.gather(
            *(h.wait() for h in self._run_handles.values()), return_exceptions=True
        )
        if self._engine is not None:
            await asyncio.shield(self._engine)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

    def _submit(self, run_id, scope_id, definition, args, kwargs, options=None, deferred=False):
        if not isinstance(definition, TaskDef):
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
                if ref._runtime is not self or ref.run_id != run_id:
                    raise ValueError("Dependencies must belong to this Runtime and run")
                ref._observed = True
            elif isinstance(ref, OutputRef):
                if ref.run_id != run_id or ref.task_id not in self._handles:
                    raise ValueError("Unknown or cross-run input reference")
            else:
                raise TypeError("Control dependencies must be TaskHandles")
        task_id = f"{run_id}/{next(self._sequence)}:{definition.name}"
        handle = TaskHandle(self, task_id, run_id)
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

    def _subflow(self, run_id, parent, definition, args, kwargs):
        definition = definition if isinstance(definition, FlowDef) else make_flow(definition)
        scope_id = f"{run_id}/flow-{next(self._sequence)}:{definition.name}"
        handle = FlowHandle(scope_id)
        self._scope_handles[scope_id] = handle
        self._post(_Message("subflow", run_id, payload=(handle, parent, definition, args, kwargs)))
        return handle

    def result_details(self, task_id):
        record = self._records.get(task_id)
        return record.result if record else None

    async def _persist(self, record=None):
        if not self._aborting:
            snapshot = self.state.snapshot()
            changes = {record.handle.id: record.view()} if record is not None else {}
            await self.state.commit(snapshot.revision, changes)

    async def _emit(self, kind, run_id=None, task_id=None, **data):
        if self._aborting:
            return
        self._event_sequence += 1
        event = RuntimeEvent(self._event_sequence, self.clock.now(), kind, run_id, task_id, data)
        await self.journal.append(event)
        self.scheduler.observe(event)
        log_event(event)

    def _spawn_scope(self, run_id, handle, parent, definition, args, kwargs):
        scope = _Scope(handle.id, run_id, handle, parent)
        self._scope_handles[scope.id] = handle
        self._scopes[scope.id] = scope
        if parent:
            self._scopes[parent].children.append(scope.id)

        async def drive_flow():
            token = _current_scope.set(scope.id)
            value, error = None, None
            try:
                context = FlowContext(self, run_id, scope.id)
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

        scope.worker = self._loop.create_task(drive_flow(), name=f"orchlet:{definition.name}")

        def cancelled_before_start(worker):
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

    async def _drive(self):
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

    async def _apply(self, message):
        kind, run_id, task_id, payload = (
            message.kind,
            message.run_id,
            message.task_id,
            message.payload,
        )
        if kind == "start_run":
            handle, definition, args, kwargs, keep_open = payload
            root = FlowHandle(f"{run_id}/root")
            self._runs[run_id] = _Run(handle, root.id, keep_open)
            self._spawn_scope(run_id, root, None, definition, args, kwargs)
            await self._emit("run_started", run_id)
        elif kind == "submit":
            record = payload
            self._records[task_id] = record
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
                handle._future.set_exception(RunClosedError("Parent flow is closed"))
            else:
                self._spawn_scope(run_id, handle, parent, definition, args, kwargs)
        elif kind == "flow_done":
            scope_id, value, error = payload
            scope = self._scopes[scope_id]
            scope.flow_done, scope.value, scope.error = True, value, error
            if error is not None:
                await self._cancel_scope_children(scope)
        elif kind == "complete":
            await self._complete(self._records[task_id], *payload)
        elif kind == "retry":
            record = self._records[task_id]
            if record.state == TaskState.RETRY_WAIT and record.attempt == payload:
                record.timer = None
                await self._ready(record)
        elif kind == "timeout":
            record = self._records[task_id]
            if record.state == TaskState.RUNNING and record.attempt == payload:
                record.timed_out = True
                record.state = TaskState.CANCELLING
                record.token.request()
                await self._persist(record)
                await self._emit("task_timeout", run_id, task_id, attempt=record.attempt)
        elif kind == "cancel_task":
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
                await self._persist()
            else:
                record = self._records[task_id]
                if record.state.terminal:
                    return False
                record.metrics.update(payload)
                await self._persist(record)
            await self._emit("metrics_updated", run_id, task_id, metrics=payload)
        elif kind == "runner_event":
            attempt, event_kind, data = payload
            record = self._records[task_id]
            if record.attempt != attempt or record.state.terminal:
                return False
            if event_kind == "metrics":
                record.metrics.update(data)
                await self._persist(record)
            await self._emit(event_kind, run_id, task_id, **data)
            return event_kind == "metrics"
        elif kind == "policy_wake":
            return payload == self._policy_generation
        else:
            raise RuntimeError(f"Unknown runtime message: {kind}")
        return True

    async def _admit(self, record):
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
        record.handle._admitted.set_result(None)
        await self._persist(record)
        await self._emit("task_submitted", record.handle.run_id, record.handle.id)

    async def _admit_deferred(self):
        for record in tuple(self._records.values()):
            if record.state == TaskState.SUBMITTED and record.deferred:
                await self._admit(record)

    async def _refresh_dependencies(self):
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

    async def _ready(self, record):
        record.state = TaskState.READY
        record.ready_at = self.clock.now()
        record.sequence = next(self._ready_sequence)
        await self._persist(record)
        await self._emit("task_ready", record.handle.run_id, record.handle.id)

    async def _schedule(self, trigger):
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
        if not isinstance(decision, ScheduleDecision) or decision.revision != snapshot.revision:
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

    async def _launch(self, record, start):
        record.state = TaskState.RUNNING
        record.attempt += 1
        record.started_at = self.clock.now()
        record.timed_out = False
        record.token = CancellationToken()
        values = {d: self._records[d].result.value for d in record.data_dependencies}
        args, kwargs = self.inputs.resolve(record.inputs, values)
        request = ExecutionRequest(
            record.handle.id,
            record.definition,
            args,
            kwargs,
            AttemptContext(
                record.handle.id, record.attempt, record.previous_error, record.previous_output
            ),
            self.clock,
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

        def emit(kind, data):
            message = _Message(
                "runner_event", record.handle.run_id, record.handle.id, (attempt, kind, data)
            )
            # Python tasks can report from a worker thread.
            self._loop.call_soon_threadsafe(self._timer_message, message)

        async def execute():
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

        record.worker = self._loop.create_task(execute(), name=f"orchlet:{record.definition.name}")
        if record.definition.timeout is not None:
            record.timer = self.clock.schedule_at(
                self.clock.now() + record.definition.timeout,
                lambda: self._timer_message(
                    _Message("timeout", record.handle.run_id, record.handle.id, attempt)
                ),
            )

    def _release(self, record):
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

    async def _complete(self, record, attempt, result, error):
        if record.attempt != attempt or record.state not in (
            TaskState.RUNNING,
            TaskState.CANCELLING,
        ):
            return
        record.worker = None
        self._release(record)
        if record.cancel_requested:
            error = TaskCancelled("Task cancelled")
        elif record.timed_out:
            error = TimeoutError(f"Task exceeded {record.definition.timeout}s timeout")
        elif isinstance(error, asyncio.CancelledError):
            error = TaskCancelled("Runner cancelled")
        if error is None and not isinstance(result, ExecutionResult):
            error = ConfigurationError("Runner.execute() must return ExecutionResult")
            result = None
        raw_text = result.raw_text if result is not None else getattr(error, "raw_text", None)
        record.attempts.append(
            Attempt(
                attempt,
                record.started_at,
                self.clock.now(),
                f"{type(error).__name__}: {error}" if error else None,
                raw_text,
            )
        )
        if record.cancel_requested:
            await self._finish(record, TaskState.CANCELLED, error=error)
        elif error is None:
            record.result = TaskResult(
                result.value,
                tuple(record.attempts),
                result.raw_text,
                result.stdout,
                result.stderr,
                result.session_id,
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

    async def _finish(self, record, state, error=None):
        record.state, record.error = state, error
        if error is not None:
            cause = error.cause if isinstance(error, TaskFailed) else error
            record.result = TaskResult(
                None,
                tuple(record.attempts),
                record.attempts[-1].raw_text if record.attempts else None,
                getattr(cause, "stdout", ""),
                getattr(cause, "stderr", ""),
                error=error,
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
        future = record.handle._future
        if not future.done():
            if error is None:
                future.set_result(record.result.value)
            else:
                future.set_exception(error)
        if not record.handle._admitted.done():
            record.handle._admitted.set_exception(error or TaskCancelled("Not admitted"))
        self._dirty.update(self._dependents.get(record.handle.id, {}))

    async def _cancel_record(self, record):
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

    async def _cancel_scope_children(self, scope):
        for node_id in scope.tasks:
            await self._cancel_record(self._records[node_id])
        for scope_id in scope.children:
            child = self._scopes[scope_id]
            if not child.flow_done:
                child.worker.cancel()
            await self._cancel_scope_children(child)

    async def _settle_scopes(self):
        changed = True
        while changed:
            changed = False
            for scope in tuple(self._scopes.values()):
                if scope.settled or not scope.flow_done:
                    continue
                tasks = [self._records[node_id] for node_id in scope.tasks]
                children = [self._scopes[scope_id] for scope_id in scope.children]
                errors = [r.error for r in tasks if r.error is not None and not r.handle._observed]
                errors += [
                    c.error for c in children if c.error is not None and not c.handle._observed
                ]
                if scope.error is None and self.failures.fail_scope(errors):
                    scope.error = errors[0]
                    await self._cancel_scope_children(scope)
                if any(not r.state.terminal for r in tasks) or any(not s.settled for s in children):
                    continue
                scope.settled = True
                if scope.error is None:
                    scope.handle._future.set_result(scope.value)
                else:
                    scope.handle._future.set_exception(scope.error)
                changed = True

    async def _settle_runs(self):
        for run_id, run in self._runs.items():
            if run.handle.done or run.keep_open or self._pending.get(run_id, 0):
                continue
            root = self._scopes[run.root]
            external = [self._records[node_id] for node_id in run.external]
            if not root.settled or any(not r.state.terminal for r in external):
                continue
            error = TaskCancelled("Run cancelled") if run.cancelled else root.error
            errors = [r.error for r in external if r.error is not None and not r.handle._observed]
            if error is None and self.failures.fail_scope(errors):
                error = errors[0]
            await self._emit("run_finished", run_id, error=str(error) if error else None)
            if error is None:
                run.handle._future.set_result(root.value)
            else:
                run.handle._future.set_exception(error)

    async def _abort(self, error):
        """Fail all affected promises and await runner shutdown even if a plugin fails."""
        self._aborting = True
        if isinstance(error, asyncio.CancelledError):
            error = TaskCancelled("Runtime stopped")
        log_runtime_error(error)
        workers = []
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
            for future in (handle._future, handle._admitted):
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
            if not handle._future.done():
                handle._future.set_exception(error)
        for handle in self._run_handles.values():
            if not handle.done:
                handle._future.set_exception(error)
        for ack in tuple(self._acks):
            if not ack.done():
                ack.set_exception(error)
        self.transport.drain(10**9)
        self._pending.clear()
