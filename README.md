# Orchlet

Orchlet is an agent orchestration library that executes Python flows directly. Nodes can be created during a run. A replaceable Scheduler combines declared priority relationships with live weights, and submissions, completions, metric updates, and timers trigger scheduling decisions.

The current runtime uses a single machine and event loop with non-preemptive scheduling. Python functions, closures, and task references remain in memory; executing a flow does not require converting it into JSON first.

**Project language.** Use English for documentation, comments, docstrings, built-in prompts, example data, and user-facing messages throughout the project.

**Install and run.** Use Python 3.14 or newer from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
python examples/conditional_routing.py
python examples/review_repair_loop.py
python examples/dynamic_singers.py --count 7
python examples/live_scheduling.py
python -m unittest discover -s tests -v
```

The singer example uses a local demo backend by default, with no account or model calls required. `--count 0` also completes successfully. Add `--codex` to make real calls using your configured local Codex CLI:

```bash
python examples/dynamic_singers.py --count 5 --codex
```

**More runnable examples.** [examples/README.md](examples/README.md) lists 11 standalone scripts and their expected behavior. They cover text validation and success/failure routing, repair loops, custom prompts and retries, dynamic fanout and backpressure, dependency policies, subflows and resource constraints, scheduling policies, external submissions and cancellation, and virtual time. All examples run offline by default.

**Console logging with Rich.** Enable the built-in [RichHandler](https://rich.readthedocs.io/en/stable/logging.html) for runtime events and application messages:

```python
from orchlet import configure_logging, get_logger

configure_logging(level="INFO")
logger = get_logger("workflow")
logger.info("Starting workflow for %s", "demo")
```

Run and task starts and completions appear at INFO, retries and timeouts at WARNING, and failures at ERROR. Use `configure_logging(level="DEBUG")` to include submissions, dependency readiness, metrics, and other detailed events. Runtime messages include run or task IDs and relevant scheduling, attempt, or failure details. Each runtime log record also exposes the original RuntimeEvent as `record.orchlet_event` for custom handlers.

Logs go to stderr with timestamps, level colors, and Rich exception rendering. Message markup is disabled, so brackets in prompts, JSON, or model output remain literal. Colors are detected from the terminal. `configure_logging(console=..., show_time=False, show_path=True)` customizes the Rich console and layout. Repeated calls replace the console handler rather than adding duplicate output.

Importing Orchlet does not configure console output. The helper configures only the `orchlet` logger namespace and preserves application handlers and the root logger. Applications can instead configure Python logging directly. All example scripts enable Rich when run as programs. MemoryEventJournal and JsonlJournal continue recording structured events independently of console logging.

**Define dynamic flows in Python.** `ctx.submit()` returns a TaskHandle. Passing a handle as another task's input establishes a dependency. `await handle` retrieves its result for subsequent Python control flow.

```python
from orchlet import EventLoopRuntime, FlowContext, flow, task


@task(priority=20)
def generate(count: int) -> list[int]:
    return list(range(count))


@task(priority=10)
def double(value: int) -> int:
    return value * 2


@task
def total(values: list[int]) -> int:
    return sum(values)


@flow
async def pipeline(ctx: FlowContext, count: int) -> int:
    values = await ctx.submit(generate, count)
    jobs = [ctx.submit(double, value) for value in values]
    return await ctx.submit(total, jobs)


runtime = EventLoopRuntime(concurrency=4)
assert runtime.run(pipeline, count=7) == 42
```

Inside an existing event loop, use `await runtime.arun(pipeline, count=7)`. Flow coordinators do not consume leaf execution slots. Use `await ctx.subflow(child_flow, ...)` for nested orchestration. A running ordinary task cannot submit child tasks through a captured FlowContext while holding an execution slot.

Input references support lists, tuples, dictionary values, and ordinary dataclass fields. Container structure is copied at submission, and references are resolved before execution. References may only point to tasks already submitted in the same run, keeping the instance dependency graph acyclic. Treat result objects as immutable; the framework does not deep-copy arbitrary user objects.

`ctx.submit()` submits work without guaranteeing an immediate start. TaskDef options such as `priority` and `resources` are fixed when an instance is created. `definition.options(priority=..., metadata=...)` returns a new definition without changing existing instances.

**Replace scheduling policies.** The default `PartialOrderScheduler` respects static priority relationships, then uses live weights to choose between equivalent or incomparable candidates. Higher numbers have higher priority by default. Equal weights use READY arrival order.

```python
from orchlet.priorities import ExplicitPriorityOrder
from orchlet.schedulers import PartialOrderScheduler, WeightedScheduler
from orchlet.weights import AgingWeight, CompositeWeightModel, MetricWeight

# Priorities can be labels. Labels without a relationship are incomparable.
partial = PartialOrderScheduler(
    order=ExplicitPriorityOrder([("urgent", "batch")]),
    weights=AgingWeight(),
)

# Numeric priority contributes to the score; live urgency can reverse its order.
weighted = WeightedScheduler(
    priority_scale=1.0,
    weights=CompositeWeightModel(
        [
            AgingWeight(rate=0.1),
            MetricWeight("urgency"),
        ]
    ),
)
runtime = EventLoopRuntime(concurrency=4, scheduler=weighted)
```

Calling `await handle.update_metrics(urgency=100)` on a READY task triggers a new scheduling decision. A Python task decorated with `@task(context=True)` receives a TaskContext as its first argument and can report metrics using `ctx.report(percent=50)`. Update run metrics with `await flow_ctx.update_metrics(...)` or `await run.update_metrics(...)`; policies read them from `snapshot.metrics[task.run_id]`.

`AgingWeight` measures time spent READY. `MetricWeight` reads task metrics. `KnownDownstreamWeight` counts known direct consumers that are waiting. `FunctionWeight` wraps a regular scoring function. You can also subclass WeightModel:

```python
from orchlet import WeightModel
from orchlet.models import ScheduleSnapshot, TaskView


class MyWeight(WeightModel):
    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        waiting = snapshot.now - (task.ready_at if task.ready_at is not None else snapshot.now)
        return waiting + 2 * float(task.metrics.get("urgency", 0))
```

With hard priority relationships, aging alone cannot move a lower-priority task ahead of a higher-priority candidate. Fairness and starvation prevention are policy choices.

**Replace the entire Scheduler.** Its input is an immutable snapshot, and its output is a batch of start decisions:

```python
from orchlet import ResourceAllocator, Scheduler
from orchlet.models import ScheduleDecision, ScheduleSnapshot, Start


class OneAtATime(Scheduler):
    def bind_resources(self, allocator: ResourceAllocator) -> None:
        self.allocator = allocator

    def schedule(self, snapshot: ScheduleSnapshot) -> ScheduleDecision:
        for node in sorted(snapshot.ready, key=lambda item: item.sequence):
            if self.allocator.plan([node], snapshot.resources) is not None:
                return ScheduleDecision(
                    snapshot.revision,
                    starts=(Start(node.id, reason="FIFO experiment"),),
                )
        return ScheduleDecision(snapshot.revision)
```

Runtime validates the revision, READY states, duplicates, and capacity for the entire batch before reserving resources and starting execution. Scheduler does not execute tasks or modify runtime state. Its `observe(event)` method can maintain policy statistics. Scheduling methods should return quickly without model calls or blocking I/O.

If weights change with time alone, return a future monotonic deadline in `ScheduleDecision(..., wake_at=...)`. Built-in schedulers also accept a positive `refresh_interval`. If a policy selects no tasks, supplies no wakeup, and leaves READY tasks with no running work, Runtime records `scheduler_waiting`. Those tasks wait for a control event or cancellation; Runtime does not override the policy.

**Configure resources, dependencies, and admission separately.**

```python
from orchlet import SubmitOptions
from orchlet.policies import AllSettled, BoundedAdmission
from orchlet.resources import TokenResourceAllocator

runtime = EventLoopRuntime(
    resources=TokenResourceAllocator(global_slots=4, gpu=1),
    admission=BoundedAdmission(max_pending=1000),
)

# Inside a flow:
# gpu_job = ctx.submit(work.options(resources={"gpu": 1}), data)
# cleanup = ctx.submit(cleanup_task, options=SubmitOptions(
#     after=(gpu_job,), dependency_policy=AllSettled(),
# ))
```

Required data references and control dependencies are checked separately. `AllSettled`, `AnySuccessful`, and `KSuccessful` only change the gate for `SubmitOptions.after`. They cannot start a task before its required input values exist. Ordinary tasks still require all data inputs to succeed.

Global execution slots belong to one Runtime and are shared across its runs. `concurrency` is shorthand for configuring the default allocator and cannot be combined with explicit `resources`. Unknown resources or requirements that exceed total capacity produce explicit failures.

A synchronous submission beyond the admission limit finishes its handle with AdmissionError. `await ctx.asubmit(...)` waits for admission capacity before returning a handle. For large fanouts, use `await ctx.map(task_def, iterable, max_in_flight=32)`; results preserve input order.

**Submit and control work during a run.**

```python
from orchlet import EventLoopRuntime, FlowDef, TaskDef


async def control[T, U](
    runtime: EventLoopRuntime, pipeline: FlowDef[[], T], urgent_task: TaskDef[[], U]
) -> U:
    async with runtime:
        run = runtime.start(pipeline, keep_open=True)
        urgent = run.submit(urgent_task)
        await urgent.update_metrics(urgency=100)
        result = await urgent
        await run.close_inputs()
        await run.wait()
        return result
```

`keep_open=True` accepts external submissions after the root flow returns. `close_inputs()` closes that entry point while accepted tasks finish. Control commands are processed in order within the same event loop. Ordinary `run()` and `arun()` calls do not require manually closing inputs.

`await handle.cancel()` requests task cancellation; `await run.cancel()` cancels the entire run. Acknowledgment means the command has been processed. Await the handle or run for actual completion. Running tasks pass through CANCELLING, and resources are released only after Runner confirms execution has stopped. Thread functions cannot be forcibly terminated, so cancellation waits for them to exit. Async functions can perform cleanup in `finally`.

**Agent calls and structured output.** An `@agent` function returns a prompt, while its task returns the decoded and validated result. Every agent must explicitly specify `backend`: either a name registered in the runtime's `backends` mapping or an `AgentBackend` instance. Omitting it is an error during definition and static type checking. Definitions can also specify result_type, a business check, and a retry policy:

```python
from dataclasses import dataclass
from orchlet import agent
from orchlet.backends import CodexBackend
from orchlet.policies import ExponentialRetry


@dataclass
class Rating:
    singer: str
    score: float


@agent(
    backend="codex",
    result_type=Rating,
    validate=lambda value: 0 <= value.score <= 10,
    retry=ExponentialRetry(max_attempts=3, delay=0.5),
)
def rate(singer: str) -> str:
    return f"Rate singer {singer}. Return a JSON object with singer and score fields in English."


runtime = EventLoopRuntime(backends={"codex": CodexBackend()})
```

**Static typing.** The project requires Python 3.14 and configures Pyright/Pylance strict mode in `pyproject.toml`. Open the `orchlet` directory as your editor workspace and select its Python 3.14+ environment. The package includes `py.typed` so downstream projects can use its annotations.

Annotate function inputs, including `ctx: FlowContext` for flows and `ctx: TaskContext` for context-aware tasks. Task return types can be inferred from their bodies or declared explicitly. Decorators preserve the result type through `.options()`, submission, awaiting, mapping, subflows, and runtime calls. An agent's prompt returns `str`; its task result is `str` by default or the type supplied through `result_type`:

```python
from typing import assert_type
from orchlet import FlowContext, TaskHandle, flow


@flow
async def ratings(ctx: FlowContext):
    job = ctx.submit(rate, "Singer A")
    assert_type(job, TaskHandle[Rating])
    assert_type(await job, Rating)
    return await ctx.map(rate, ["Singer A", "Singer B"])


assert_type(runtime.run(ratings), list[Rating])
```

`result_type` supports classes, parameterized containers such as `list[str]`, unions, `Literal`, and `Annotated`. `TypeForm` from `typing_extensions` preserves these types for the checker; the project enables experimental features for Pylance releases that still require this setting for TypeForm. Result types and validator callbacks are linked when `result_type` is supplied. Without it, annotate a task's validation callback explicitly; a decorator factory cannot infer its callback parameter from a function it has not received yet.

`run()`, `arun()`, and `subflow()` also check flow arguments. `submit()`, `asubmit()`, `map()`, and external submissions preserve result types but accept dynamically resolved inputs: Python's typing system cannot express replacing every nested input value with a task handle. `start()` also accepts dynamic arguments because it adds the `keep_open` option. Metadata, backend payloads, and task lookup by a string ID have dynamic types. These boundaries are explicit; strict mode does not imply that all uses of `Any` are forbidden.

Run `pyright` after installing the dev extra. It checks the library, examples, tests, and the `assert_type` regressions in `typecheck/inference.py`. The test suite also checks that invalid result access and incorrect flow arguments produce diagnostics.

The default str result uses TextCodec. Other result_type values use JsonCodec and Pydantic TypeAdapter. Type adaptation follows Pydantic's default conversion rules; use a custom ResultValidator or a strict TypeValidator for stricter checks. Business checks fail by returning False or raising an exception. Other return values mean success and do not replace the result; use ResultValidator for transformations.

A task publishes success only after decoding, type validation, and business checks all pass. Failed agent responses retain their raw text, and retry prompts include the previous response and error. `max_attempts` includes the initial attempt. Backoff does not occupy an execution slot. `timeout` sets the deadline for requesting cancellation; actual completion also depends on Runner confirming shutdown.

`CodexBackend` invokes the local CLI with argv and stdin, uses a read-only sandbox by default, and reads the final response from the output file. It supports `FixedSession(existing_session_id)`; tasks sharing a session execute exclusively within one Runtime. FreshSession creates a new session for each attempt by default. Automatic named shared sessions and steering a running agent are not currently exposed as APIs.

`CommandBackend([...])` sends the prompt through stdin and uses stdout as the final response, without invoking a shell. Tests and custom SDK integrations can subclass AgentBackend, implement `run_turn()`, and return AgentReply.

**Results, failures, and events.** `await handle` returns the business value. `handle.snapshot` exposes task state. `handle.details` exposes TaskResult, including attempts, raw_text, stdout, stderr, session_id, and error. Execution failures raise TaskFailed with the original exception in cause. Skipped, cancelled, and rejected tasks have corresponding error types.

After catching a task failure, a flow can submit new repair tasks. A normally returning flow still joins its children. By default, an unhandled failure fails its scope and cancels remaining children. `ctx.all_settled(handles)` returns Outcome objects for explicitly collecting partial results. `CollectFailures` changes scope handling of unobserved errors; directly awaiting a failed handle still raises.

MemoryStateStore and MemoryEventJournal retain snapshots and events during execution. `JsonlJournal(path)` records diagnostic events. Use `runtime.journal.read()` to inspect start order, scheduling reasons, retries, and state changes. Complete business results remain in memory. The journal is not a recovery checkpoint. Transparent recovery of Python flows after process crashes, preemption, and distributed execution are not currently supported.

**Extension points and implementation locations.** All abstract interfaces live in `src/orchlet/contracts.py` and use ABC. Implementations are injected through Runtime or Scheduler constructors, without a global plugin singleton.

| Modules | Main extension points or implementations |
| --- | --- |
| `definitions.py`, `runtime.py` | FlowController, Runtime, TaskDef, FlowContext |
| `inputs.py`, `policies.py` | InputResolver, DependencyPolicy, AdmissionPolicy, RetryPolicy, FailurePolicy |
| `priorities.py`, `weights.py`, `schedulers.py` | PriorityOrder, WeightModel, ReadyIndex, Scheduler |
| `resources.py` | ResourceAllocator |
| `runners.py`, `backends.py` | Runner, AgentBackend |
| `prompts.py`, `outputs.py`, `sessions.py` | PromptBuilder, OutputCodec, ResultValidator, SessionPolicy |
| `state.py`, `events.py`, `clocks.py` | StateStore, EventTransport, EventJournal, Clock |
| `logging.py` | Rich console setup, named loggers, and runtime event logging |

Task state progresses through submission, dependency waiting, READY, and RUNNING, then success, failure, or RETRY_WAIT. Tasks whose required inputs fail become SKIPPED. Runtime is the only state writer, and attempt numbers prevent stale completion events from overwriting later attempts.

**Repeatable policy experiments.** SimulatedRunner supplies deterministic results and durations from a request. Experiments advance VirtualClock explicitly with `advance()` or `advance_to_next()`. Wait until `clock.next_deadline` is set before advancing to study time-dependent policies without real model calls. VirtualClock does not advance real I/O.

Tests cover dynamic nodes, priority relationships, live weights, shared resources, cancellation cleanup, output repair, session exclusion, external submissions, and abstract component contracts. Codex tests use a fake process and do not make model calls.

```bash
python -m pip install -e '.[dev]'
ruff check .
ruff format --check .
pyright --warnings
python -m unittest discover -s tests -v
```

**Git commit checks.** Install the [pre-commit](https://pre-commit.com/) hook once per clone, from a Python 3.14+ development environment:

```bash
python -m pip install -e '.[dev]'
pre-commit install
pre-commit run --all-files
```

Every commit runs `ruff format .`, `ruff check .`, and `pyright --warnings`, in that order. The checks cover the whole project, including commits that only change configuration or documentation. A formatter change stops the commit so you can review and stage the formatted files before committing again. Ruff violations, Pyright errors, and Pyright warnings also stop the commit.

The hooks use isolated Python 3.14 environments with pinned checker versions, so commits from VS Code do not depend on activating a shell environment. Python 3.14 must be available when the hook environments are created. Keep the hook's analysis dependencies aligned with `pyproject.toml` when changing project dependencies. Pre-commit temporarily stashes unstaged tracked changes while checking the staged version.

Local hooks can be bypassed with `git commit --no-verify`. To enforce these checks on the shared branch, run them in CI and require the successful CI status in branch protection.
