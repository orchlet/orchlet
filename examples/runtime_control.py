"""Submit work during a run, update metrics, cancel tasks, handle timeouts, and close inputs."""

import argparse
import asyncio

from orchlet import EventLoopRuntime, FlowContext, configure_logging, flow, get_logger, task
from orchlet.errors import TaskCancelled, TaskFailed
from orchlet.events import JsonlJournal
from orchlet.runners import TaskContext
from orchlet.schedulers import WeightedScheduler
from orchlet.weights import MetricWeight

logger = get_logger("examples.runtime_control")


@flow
async def service(ctx: FlowContext) -> str:
    # keep_open=True accepts external submissions after the root flow returns.
    return "Root flow finished"


async def main(journal_path: str | None = None) -> None:
    started = asyncio.Event()

    @task(context=True)
    async def wait_for_input(task_ctx: TaskContext, label: str) -> None:
        logger.info(f"{label}: started")
        task_ctx.report(percent=10)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            # Cancellation and timeout wait for cleanup before releasing the execution slot.
            await asyncio.sleep(0.005)
            logger.info(f"{label}: cleanup finished")

    @task
    async def urgent() -> str:
        logger.info("urgent: acquired the execution slot and started")
        return "Urgent task completed"

    runtime = EventLoopRuntime(
        concurrency=1,
        scheduler=WeightedScheduler(weights=MetricWeight("urgency")),
        journal=JsonlJournal(journal_path) if journal_path else None,
    )
    async with runtime:
        run = runtime.start(service, keep_open=True)
        slow = run.submit(wait_for_input, "slow")
        await started.wait()

        extra = run.submit(urgent)
        await extra.update_metrics(urgency=100)
        snapshot = extra.snapshot
        assert snapshot is not None
        logger.info(f"New task state: {snapshot.state.value}; updated metrics: {snapshot.metrics}")
        logger.info("Cancelling slow; urgent will start after cleanup finishes")
        await slow.cancel()
        try:
            await slow
        except TaskCancelled:
            logger.info("slow: cancellation completed")
        logger.info(await extra)

        timed = run.submit(wait_for_input.options(timeout=0.02), "timed")
        try:
            await timed
        except TaskFailed as exc:
            if not isinstance(exc.cause, TimeoutError):
                raise
            logger.warning("timed: caught timeout; %s", exc.cause)

        await run.close_inputs()
        logger.info(f"Run completed after closing inputs: {await run}")
        logger.info(f"Final task states: {[view.state.value for view in run.tasks]}")
    if journal_path:
        logger.info(f"Diagnostic events appended to: {journal_path}")


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", help="Optional path for appending diagnostic events as JSONL")
    args = parser.parse_args()
    asyncio.run(main(args.journal))
