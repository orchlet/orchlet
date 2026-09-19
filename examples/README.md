# Orchlet examples

Run these commands from the repository root with a Python 3.14+ environment active:

```bash
python -m pip install -e .
python examples/conditional_routing.py
```

All examples run offline by default. Agent examples use DemoBackend with preset responses, while Python tasks use local computation or short async waits. The `gpu` resource in the GPU example is a token and requires no physical GPU. Only `dynamic_singers.py --codex` invokes a real Codex backend.

Every script enables Rich logging through `configure_logging()` and writes its progress and results with `get_logger(...)`. Logs go to stderr and include timestamps, severity, and runtime lifecycle events. The snippets below describe message content; Rich adds the log columns and wraps long lines to the console width. To include detailed readiness and metric events, change the script's setup to `configure_logging(level="DEBUG")`. JSONL output from `runtime_control.py --journal ...` remains structured event data.

| Example | Demonstrated capabilities | Command |
| --- | --- | --- |
| [conditional_routing.py](conditional_routing.py) | Text validation; route success and failure to different agents | `python examples/conditional_routing.py` |
| [review_repair_loop.py](review_repair_loop.py) | Review, repair, and review again; dynamic nodes; bounded rounds | `python examples/review_repair_loop.py` |
| [prompt_and_retry.py](prompt_and_retry.py) | Programmable prompts; custom PromptBuilder; type and business validation; retry feedback and history | `python examples/prompt_and_retry.py` |
| [dynamic_singers.py](dynamic_singers.py) | Dynamic fanout from an agent's list; aggregate structured results | `python examples/dynamic_singers.py --count 7` |
| [bounded_fanout.py](bounded_fanout.py) | Dynamic workload; map window; admission backpressure; task progress; ordered results | `python examples/bounded_fanout.py` |
| [dependency_gates.py](dependency_gates.py) | AnySuccessful and AllSettled control dependencies; collect partial results | `python examples/dependency_gates.py` |
| [resources_and_subflows.py](resources_and_subflows.py) | Concurrent subflows; nested result references; shared resource capacity | `python examples/resources_and_subflows.py` |
| [scheduler_policies.py](scheduler_policies.py) | Numeric and partial-order priorities; weights; custom Scheduler and WeightModel | `python examples/scheduler_policies.py` |
| [live_scheduling.py](live_scheduling.py) | Update READY task metrics during execution to change subsequent scheduling | `python examples/live_scheduling.py` |
| [runtime_control.py](runtime_control.py) | External submissions, metrics, cancellation, timeouts, input closure, and logging | `python examples/runtime_control.py` |
| [virtual_time.py](virtual_time.py) | Replace Clock and Runner to simulate durations and inspect event timelines | `python examples/virtual_time.py` |

**Start with success and failure routing.** `conditional_routing.py` runs both scenarios by default. Select a single branch with:

```bash
python examples/conditional_routing.py --case success
python examples/conditional_routing.py --case failure
```

`validate` checks ordinary text and raises when the check fails. Successful `await job` execution enters `else`; an execution failure enters `except TaskFailed`. The failure branch reads the response from `job.details.raw_text` and the exception from `exc.cause`. Passing a failed handle as a required data input would skip the repair task. If `retry` is configured, the failure branch runs after automatic retries stop.

**Compare automatic retries with repair nodes.**

```bash
python examples/prompt_and_retry.py
python examples/review_repair_loop.py
python examples/review_repair_loop.py --stubborn --max-rounds 3
```

In `prompt_and_retry.py`, the same task receives invalid JSON, an out-of-range score, and finally a valid result. Each call prints its prompt. The final result is `Rating(singer='Singer A', score=8.5)`, followed by all three attempt records. The default FunctionPromptBuilder already appends error feedback; replacing it here demonstrates control over the feedback format.

`review_repair_loop.py` uses a Python loop to create a new review task each round and a repair task after rejection. The default scenario passes on round 2. `--stubborn` simulates ineffective repairs and returns `approved=False` at the round limit. Rejection is an explicitly handled business result, so the script exits normally. Feedback is passed through prompt arguments without depending on a shared agent session.

**Adjust fanout windows and resource capacity.**

```bash
python examples/bounded_fanout.py --count 20 --window 6 --concurrency 4 --max-pending 2
python examples/bounded_fanout.py --count 0
python examples/resources_and_subflows.py --slots 1
```

The upstream task in `bounded_fanout.py` filters candidate items, and the actual list determines downstream tasks. The map window limits submitted work whose results have not been collected. Admission limits accepted tasks that have not finished, while execution slots limit actively running tasks. The default execution peak is 2. Completion order can vary, but returned values always follow input order. `--count 0` demonstrates empty fanout.

`resources_and_subflows.py` creates two subflows, each with two simulated inference tasks. They share one GPU token, so peak GPU task concurrency is always 1. Even with a single execution slot, subflows do not hold that slot while waiting for their children.

**Compare scheduling policies.** Expected order in `scheduler_policies.py`:

```text
Hard priorities: urgent -> fast -> slow
Weighted priorities: fast -> urgent -> slow
Custom shortest-job-first policy: fast -> slow -> urgent
Partial order (urgent > batch; interactive is incomparable): interactive -> urgent -> batch
```

In the partial-order case, `urgent` outranks `batch`, while `interactive` is incomparable with both. Even with its high weight, `batch` follows `urgent`. The script also prints scheduling reasons from the journal. To see metrics change during execution, run `live_scheduling.py`: updating B's urgency moves it ahead of A. Scheduling is non-preemptive, so changing READY order does not interrupt running tasks.

**Control runs and inspect events.**

```bash
python examples/runtime_control.py
python examples/runtime_control.py --journal /tmp/orchlet-demo-events.jsonl
python examples/virtual_time.py
```

`runtime_control.py` keeps external submissions open, cancels a waiting task so urgent work can execute, and then demonstrates timeout cleanup. Final task states are `cancelled`, `succeeded`, and `failed`, in that order. `--journal` appends diagnostic events to the chosen file; otherwise events stay in memory. This journal supports inspection rather than crash recovery.

`virtual_time.py` manually advances the clock to 120, 720, and 750 seconds, completes three simulated tasks, and prints their start and finish timeline. Its single execution slot and task chain demonstrate replaceable Clock and Runner implementations. The loop is not a general simulation driver for arbitrary concurrent workloads.
