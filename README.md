# Orchlet

Orchlet is an agent orchestration library that executes Python flows directly. Nodes can be created during a run. A replaceable Scheduler combines declared priority relationships with live weights, and submissions, completions, metric updates, and timers trigger scheduling decisions.

The current runtime uses a single machine and event loop with non-preemptive scheduling. Flows execute as Python functions without conversion into JSON. Run state, successful task results, and agent execution artifacts are saved locally by default, so repeating a command can continue an interrupted or failed run.

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

Run and task starts and completions, resumed runs, and restored task results appear at INFO, retries and timeouts at WARNING, and failures at ERROR. Use `configure_logging(level="DEBUG")` to include submissions, dependency readiness, metrics, and other detailed events. Runtime messages include run or task IDs and relevant scheduling, attempt, or failure details. Each runtime log record also exposes the original RuntimeEvent as `record.orchlet_event` for custom handlers.

Logs go to stderr with timestamps, level colors, and Rich exception rendering. Message markup is disabled, so brackets in prompts, JSON, or model output remain literal. Colors are detected from the terminal. `configure_logging(console=..., show_time=False, show_path=True)` customizes the Rich console and layout. Repeated calls replace the console handler rather than adding duplicate output.

Importing Orchlet does not configure console output. The helper configures only the `orchlet` logger namespace and preserves application handlers and the root logger. Applications can instead configure Python logging directly. All example scripts enable Rich when run as programs. Event journals and the default artifact store record runtime events independently of console logging; enabling Rich is not required for persistence.

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

**Stable keys for dynamic nodes.** Give a submission a logical key when its position can change during recovery. Use `SubmitOptions(key=...)` for tasks and a context view for subflows:

```python
from orchlet import FlowContext, SubmitOptions, flow, task


@task
def review(bug_id: str) -> str:
    return f"Reviewed {bug_id}"


@flow
async def process_bug(ctx: FlowContext, bug_id: str) -> str:
    return await ctx.submit(review, bug_id, options=SubmitOptions(key="review"))


@flow
async def bug_reviews(ctx: FlowContext, bug_ids: list[str]) -> list[str]:
    children = [
        ctx.with_options(SubmitOptions(key=f"bug:{bug_id}")).subflow(process_bug, bug_id)
        for bug_id in bug_ids
    ]
    return [await child for child in children]
```

Keys must be nonempty strings, unique among all task and subflow submissions in the same parent scope. Different parents can reuse a key, so every bug branch can have its own `"review"` task. Submitting the same key twice raises `ValueError`, even after the first node finishes; keep its handle to await the same node again. Retries remain attempts of that node.

`ctx.with_options(...)` returns a view of the same scope with immutable submission defaults. It does not change the original context or create a node. `submit()` and `asubmit()` apply those defaults; an explicit `options=` replaces them entirely. Calling `with_options()` again also replaces the defaults. A child flow receives its own ordinary context. Subflows support only the `key` option; `after` and `dependency_policy` apply to task submissions. The view keeps `subflow()` argument checking intact, including business arguments named `key` or `options`.

`run.submit(..., options=SubmitOptions(key=...))` also supports keys for external work. A context view used for `map_flows()` or `map_flows_settled()` gives the batch itself a key; the mapping's `key=` callback still identifies its members. Each submission consumes its key, so create a view with a different key for each independent node or batch.

Keys are scoped by the parent's identity: key every ancestor whose submission position can vary. Unkeyed nodes still use position, and keyed submissions do not advance their positional counters. `TaskHandle.key` and `FlowHandle.key` expose the original key, which is also saved in task and scope metadata. Within a matching unfinished run, compatible successful tasks keep their IDs and checkpoints when reordered. Existing definition, input, dependency, and result checks still apply; reusing a key with incompatible work raises `RecoveryError`. Adding keys to an old positional workflow changes its identities; start that transition with `resume=False`.

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

`run()`, `arun()`, and `subflow()` also check flow arguments. `map_flows()` and `map_flows_settled()` check the work-item type and infer the result type. `submit()`, `asubmit()`, `map()`, and external submissions preserve result types but accept dynamically resolved inputs: Python's typing system cannot express replacing every nested input value with a task handle. `start()` also accepts dynamic arguments because it adds the `keep_open` option. Metadata, backend payloads, and task lookup by a string ID have dynamic types. These boundaries are explicit; strict mode does not imply that all uses of `Any` are forbidden.

Run `pyright` after installing the dev extra. It checks the library, examples, tests, and the `assert_type` regressions in `typecheck/inference.py`. The test suite also checks that invalid result access and incorrect flow arguments produce diagnostics.

The default str result uses TextCodec. Other result_type values use JsonCodec and Pydantic TypeAdapter. Type adaptation follows Pydantic's default conversion rules; use a custom ResultValidator or a strict TypeValidator for stricter checks. Business checks fail by returning False or raising an exception. Other return values mean success and do not replace the result; use ResultValidator for transformations.

A task publishes success only after execution, any configured decoding and validation, and saving its result checkpoint. Failed agent responses retain their raw text, and retry prompts include the previous response and error. `max_attempts` includes the initial attempt. Backoff does not occupy an execution slot. `timeout` sets the deadline for requesting cancellation; actual completion also depends on Runner confirming shutdown.

`CodexBackend` invokes the local CLI with argv and stdin, uses a read-only sandbox by default, and reads the final response from the output file. It supports `FixedSession(existing_session_id)`; tasks sharing a session execute exclusively within one Runtime. FreshSession creates a new session for each attempt by default. Automatic named shared sessions and steering a running agent are not currently exposed as APIs.

`CommandBackend([...])` sends the prompt through stdin and uses stdout as the final response, without invoking a shell. Tests and custom SDK integrations can subclass AgentBackend, implement `run_turn()`, and return AgentReply.

**Results, failures, and events.** `await handle` returns the business value. `handle.snapshot` exposes task state. `handle.details` exposes TaskResult, including attempts, raw_text, stdout, stderr, session_id, exit_code, artifacts, and error. Each Attempt retains its own diagnostics and artifact paths. Execution failures raise TaskFailed with the original exception in cause. Skipped, cancelled, and rejected tasks have corresponding error types.

After catching a task failure, a flow can submit new repair tasks. A normally returning flow still joins its children. By default, an unhandled failure fails its scope and cancels remaining children. `ctx.all_settled(handles)` accepts task and flow handles from the same runtime and run, and returns typed Outcome objects for explicitly collecting partial results. `Outcome.node_id` identifies either kind of member; the original `task_id` attribute remains available. `CollectFailures` changes scope handling of unobserved errors; directly awaiting a failed handle still raises.

**Subflow batches.** Map a flow taking one typed work item over an iterable. Each branch advances independently, and returned results follow input order. A work-item dataclass can carry several inputs.

```python
from orchlet import FlowContext, flow, task


@task
async def double(value: int) -> int:
    return value * 2


@flow
async def process(ctx: FlowContext, value: int) -> int:
    return await ctx.submit(double, value)


@flow
async def pipeline(ctx: FlowContext) -> list[int]:
    return await ctx.map_flows(process, range(10), key=str)
```

`map_flows()` returns `list[T]` on success. Its default `WaitAllThenRaise()` policy continues submitting members and waits for all of them before raising `BatchFailed`. Each entry in `error.failures` contains the member's `key`, `flow_id`, original `error`, and a `task_id` when the cause identifies a failing task. The aggregate's cause is an `ExceptionGroup`. A successful `False` or `None` value is an ordinary success.

Pass `failure=FailFast()` to stop submitting members after a terminal member failure, cancel the batch's unfinished members, and wait for their cleanup. Task retries finish before the batch sees a failure. Batch cancellation affects its own descendants; an uncaught aggregate then propagates through the usual parent-scope failure rules.

Use `await ctx.map_flows_settled(process, items, key=...)` to return `list[Outcome[T]]` containing both successful values and ordinary execution failures. Batch outcomes include their logical `key`. Iterator errors, invalid keys, and caller cancellation still propagate. A flow that handles member failures and returns normally is successful, so repeating that completed run starts fresh.

Both mapping APIs default to `max_in_flight=None`: they impose no additional branch window. Set a positive integer to bound submitted but unfinished subflows. A completed branch frees one place immediately. This window is independent of running-agent resource limits; tasks still use the scheduler, resource allocator, and admission policy. As with other submissions, use `ctx.asubmit()` inside a member when it should wait for global admission capacity instead of receiving `AdmissionError`. A small window also limits which work the scheduler can see. Mapping retains collected results and the runtime retains execution history, even with a bounded window.

Keys must be unique, nonempty strings within a batch; omitted keys use input positions. Explicit keys keep member identities stable when their replay order varies within the reconstructed batch. Existing invocation and input checks still apply. Give the batch a stable parent identity with `ctx.with_options(SubmitOptions(key="bugs")).map_flows(...)` if its own position can vary. Use submission keys inside members for nodes that can be reordered; unkeyed submissions must retain their relative order. Recovery rejects incompatible member inputs, missing previously submitted members, and new keys in a batch whose input was already exhausted. Successful leaf checkpoints are reused; controllers are replayed. Batch records in `batches/` and `batch_*` events preserve membership, status, and error causes.

For custom behavior, implement `BatchFailurePolicy.decide(snapshot)`. `BatchSnapshot` supplies the batch ID, submitted and settled counts, and failures observed so far. Return `BatchDecision.CONTINUE`, `STOP` (stop admission and drain current members), or `CANCEL` (stop admission and cancel unfinished members). Runtime performs state changes and cancellation. Use explicit work-item inputs for changing data; arbitrary strategy state and captured closure state are not checkpointed.

`FlowHandle` exposes `run_id`, `done`, and `cancel()`. Cancellation requests propagate through descendants; await the handle or collect it with `all_settled()` to wait for cleanup. Cancelling a mapping call also cancels its owned batch and waits for cleanup. Cancelling an ordinary handle waiter merely detaches that waiter.

MemoryStateStore and MemoryEventJournal retain snapshots and events during execution. `JsonlJournal(path)` is an optional diagnostic journal. Use `runtime.journal.read()` to inspect events from the current runtime instance. The artifact store separately saves checkpoints and a run's `events.jsonl`, which retains event history across restarts. An event journal alone is not a recovery checkpoint. Preemption and distributed execution are not currently supported.

**Durable runs and automatic recovery.** After an interruption or a failed run, repeat the same command from the same working directory with the same flow and explicit inputs. Orchlet automatically opens the matching unfinished run. Successful tasks return their saved values, while failed or interrupted tasks receive new attempts. Restarting permits another attempt even if the earlier run exhausted automatic retries. Attempt numbers continue increasing, preserving previous logs. Repeating a fully successful run starts a new run.

```python
from orchlet import EventLoopRuntime, FileArtifactStore

# These are the defaults; choose another directory to customize storage.
runtime = EventLoopRuntime(
    artifacts=FileArtifactStore(".orchlet/runs"),
    resume=True,
)
result = runtime.run(pipeline, count=7)

# Force a new run while retaining previous run files.
fresh_runtime = EventLoopRuntime(resume=False)
```

Automatic matching uses the working directory, command arguments, flow identity, explicit inputs, and the order of repeated invocations within a runtime. Set `run_key="my-workflow"` to provide a lookup key independently of the command; flow and input checks still apply. Recovery validates saved flow and task signatures, submission identities (keys or positions), inputs, and result checkpoints. Detected mismatches or damaged results raise RecoveryError; use `resume=False` when intentionally starting over. Node keys do not change run matching or reopen completed runs. A run that handles its task failures and returns successfully is considered complete.

The default store uses `.orchlet/runs/<run_id>/`:

| Files within a run | Contents |
| --- | --- |
| `run.json` | Run status, inputs, metrics, timestamps, and errors |
| `events.jsonl` | Scheduling, execution, retry, and recovery events |
| `scopes/`, `slots/` | Flow and submission metadata for replay checks |
| `batches/` | Batch configuration, stable member identities, outcomes, and failure causes |
| `tasks/<task>/task.json` | Task state, inputs, dependencies, and attempt history |
| `tasks/<task>/value.pickle` | Successful task value serialized by the checkpoint codec |
| `tasks/<task>/attempts/001/`, `002/`, ... | Separate files for every execution attempt |

Attempt files include `request.json` and `result.json`, plus agent `prompt.txt`, `stdout.log`, `stderr.log`, `output.txt`, and `launch.json` when available. CodexBackend and CommandBackend write subprocess stdout and stderr directly to files while running. Codex JSON execution events remain in `stdout.log`, and its final response is saved separately in `output.txt`. Custom backends can use `request.artifacts` to persist output during execution; returned AgentReply output is saved by AgentRunner before validation.

Use `run.artifacts_dir` or `ctx.artifacts_dir` for the run directory, `handle.artifacts_dir` for a task directory, and `handle.details.artifacts` for the latest attempt's paths. Keep the store's `.index` directory alongside the run directories for automatic discovery. Run locks prevent concurrent processes from continuing the same invocation; on POSIX, recovery also refuses to race an agent subprocess left running after its parent was killed.

Recovery replays Python flow controllers to reconstruct dynamic submissions. Use stable keys when submission order can vary, keep unkeyed submissions in deterministic relative order within each flow, pass changing data as explicit inputs, and put side effects inside tasks. An interruption after an external effect but before the success checkpoint can cause that task to execute again, so tasks must tolerate repeated execution. Files written by tasks, closure state, and arbitrary controller or plugin state are not automatically snapshotted. For `keep_open=True`, callers must resubmit external work when reconstructing the run.

The default PickleCheckpointCodec preserves Python result types. Results must be serializable, and their class definitions must remain available on restart. Provide `checkpoint_codec=...` with a CheckpointCodec implementation for another representation. Pickle can execute code when loaded, so resume only from trusted local run files. The default `.orchlet/` directory is ignored by this repository's Git configuration.

**Extension points and implementation locations.** All abstract interfaces live in `src/orchlet/contracts.py` and use ABC. Implementations are injected through Runtime or Scheduler constructors, without a global plugin singleton.

| Modules | Main extension points or implementations |
| --- | --- |
| `definitions.py`, `runtime.py` | FlowController, Runtime, TaskDef, FlowContext |
| `inputs.py`, `policies.py` | InputResolver, DependencyPolicy, AdmissionPolicy, RetryPolicy, FailurePolicy |
| `batches.py`, `policies.py` | BatchFailurePolicy, WaitAllThenRaise, FailFast |
| `priorities.py`, `weights.py`, `schedulers.py` | PriorityOrder, WeightModel, ReadyIndex, Scheduler |
| `resources.py` | ResourceAllocator |
| `runners.py`, `backends.py` | Runner, AgentBackend |
| `prompts.py`, `outputs.py`, `sessions.py` | PromptBuilder, OutputCodec, ResultValidator, SessionPolicy |
| `state.py`, `events.py`, `clocks.py` | StateStore, EventTransport, EventJournal, Clock |
| `artifacts.py`, `checkpoints.py` | ArtifactStore, FileArtifactStore, CheckpointCodec, PickleCheckpointCodec |
| `logging.py` | Rich console setup, named loggers, and runtime event logging |

Task state progresses through submission, dependency waiting, READY, and RUNNING, then success, failure, or RETRY_WAIT. Tasks whose required inputs fail become SKIPPED. Runtime is the only state writer, and attempt numbers prevent stale completion events from overwriting later attempts.

**Repeatable policy experiments.** SimulatedRunner supplies deterministic results and durations from a request. Experiments advance VirtualClock explicitly with `advance()` or `advance_to_next()`. Wait until `clock.next_deadline` is set before advancing to study time-dependent policies without real model calls. VirtualClock does not advance real I/O.

Tests cover dynamic nodes, priority relationships, live weights, shared resources, cancellation cleanup, output repair, session exclusion, external submissions, and abstract component contracts. Recovery tests also cover process termination followed by the same command, saved result types, live subprocess logs, replay mismatches, corrupted checkpoints, and run locks. Codex tests use a fake process and do not make model calls.

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
