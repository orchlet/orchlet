"""A live metric change lets a lower-priority READY task run first."""

import asyncio

from orchlet import configure_logging, get_logger, EventLoopRuntime, flow, task
from orchlet.schedulers import WeightedScheduler
from orchlet.weights import MetricWeight

logger = get_logger("examples.live_scheduling")


@flow
async def pipeline(ctx):
    started, release = asyncio.Event(), asyncio.Event()
    order = []

    @task(priority=1000)
    async def blocker():
        started.set()
        await release.wait()

    @task
    async def work(name):
        order.append(name)
        return name

    busy = ctx.submit(blocker)
    first = ctx.submit(work.options(priority=10), "A: priority=10")
    second = ctx.submit(work.options(priority=0), "B: priority=0, urgency=100")

    await started.wait()
    await second.update_metrics(urgency=100)
    release.set()
    await ctx.all_settled([busy, first, second])
    return order


if __name__ == "__main__":
    configure_logging()
    runtime = EventLoopRuntime(
        concurrency=1,
        scheduler=WeightedScheduler(weights=MetricWeight("urgency")),
    )
    logger.info("Execution order:")
    for name in runtime.run(pipeline):
        logger.info(name)
