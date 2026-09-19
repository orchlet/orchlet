"""Continue after any source succeeds, clean up after all settle, and collect partial results."""

import asyncio

from orchlet import (
    EventLoopRuntime,
    FlowContext,
    SubmitOptions,
    configure_logging,
    flow,
    get_logger,
    task,
)
from orchlet.policies import AllSettled, AnySuccessful

logger = get_logger("examples.dependency_gates")


@flow
async def pipeline(ctx: FlowContext) -> list[str]:
    allow_remote_to_finish = asyncio.Event()

    @task
    async def cached() -> str:
        logger.info("cache: succeeded")
        return "cached result"

    @task
    async def broken() -> str:
        logger.warning("broken: failed")
        raise ValueError("This data source is temporarily unavailable")

    @task
    async def remote() -> str:
        await allow_remote_to_finish.wait()
        logger.info("remote: succeeded")
        return "remote result"

    @task
    async def continue_early() -> None:
        logger.info("AnySuccessful: a source succeeded; continue before the remote source finishes")
        allow_remote_to_finish.set()

    @task
    async def cleanup() -> None:
        logger.info("AllSettled: every source has finished; running cleanup")

    sources = (ctx.submit(cached), ctx.submit(broken), ctx.submit(remote))
    early = ctx.submit(
        continue_early,
        options=SubmitOptions(after=sources, dependency_policy=AnySuccessful()),
    )
    finalizer = ctx.submit(
        cleanup,
        options=SubmitOptions(after=sources, dependency_policy=AllSettled()),
    )
    # after is a control dependency. Passing sources as input would require all to succeed.
    await early
    outcomes = await ctx.all_settled(sources)
    await finalizer
    for label, outcome in zip(("cache", "broken", "remote"), outcomes):
        if outcome.succeeded:
            logger.info(f"  {label}: kept result {outcome.value!r}")
        else:
            logger.warning("%s: recorded error %s", label, outcome.error)
    return [
        outcome.value for outcome in outcomes if outcome.succeeded and outcome.value is not None
    ]


if __name__ == "__main__":
    configure_logging()
    result = EventLoopRuntime(concurrency=3).run(pipeline)
    logger.info(f"Partial results: {result}")
