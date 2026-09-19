"""Simulate a 750-second task chain with VirtualClock and SimulatedRunner."""

import asyncio

from orchlet import EventLoopRuntime, FlowContext, configure_logging, flow, get_logger, task
from orchlet.clocks import VirtualClock
from orchlet.runners import SimulatedRunner

logger = get_logger("examples.virtual_time")


@task(kind="simulation")
def work(value: str) -> str:
    # SimulatedRunner supplies durations and results without executing this function body.
    raise AssertionError("This example must use SimulatedRunner")


@flow
async def pipeline(ctx: FlowContext) -> str:
    prepared = ctx.submit(work.options(name="prepare", metadata={"seconds": 120}), "data")
    trained = ctx.submit(work.options(name="train", metadata={"seconds": 600}), prepared)
    return await ctx.submit(work.options(name="evaluate", metadata={"seconds": 30}), trained)


async def main() -> None:
    clock = VirtualClock()
    runtime = EventLoopRuntime(
        concurrency=1,
        clock=clock,
        runners={
            "simulation": SimulatedRunner(
                result=lambda request: f"{request.args[0]} -> {request.definition.name}",
                duration=lambda request: request.definition.metadata["seconds"],
            )
        },
    )
    async with runtime:
        run = runtime.start(pipeline)
        # This example has one execution slot and one chain. Let workers register timers.
        # Concurrent simulations also need to coordinate events at the same virtual time.
        while not run.done:
            await asyncio.sleep(0)
            if clock.next_deadline is not None:
                logger.info(f"Advancing virtual time: {clock.now():g}s -> {clock.next_deadline:g}s")
                clock.advance_to_next()
        logger.info(f"Final result: {await run}")
    for event in runtime.journal.read():
        if event.kind in {"task_started", "task_finished"}:
            assert event.task_id is not None
            name = event.task_id.rsplit(":", 1)[-1]
            logger.info(f"  t={event.time:5g}s  {name:8}  {event.kind}")
    logger.info(f"Total virtual duration: {clock.now():g}s")


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
