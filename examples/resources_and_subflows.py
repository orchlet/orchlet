"""Share execution slots and one GPU token across two subflows using simulated computation."""

import argparse
import asyncio

from orchlet import configure_logging, get_logger, EventLoopRuntime, flow, task
from orchlet.resources import TokenResourceAllocator

logger = get_logger("examples.resources_and_subflows")


@task
async def prepare(label):
    logger.info(f"Preparing input: {label}")
    await asyncio.sleep(0.005)
    return label.upper()


@task
def collect(values):
    return values


@flow
async def pipeline(ctx):
    active_gpu, peak_gpu = 0, 0

    @task(resources={"gpu": 1})
    async def infer(label):
        nonlocal active_gpu, peak_gpu
        active_gpu += 1
        peak_gpu = max(peak_gpu, active_gpu)
        logger.info(f"GPU started: {label}; active GPU tasks={active_gpu}")
        try:
            await asyncio.sleep(0.01)
            return f"Inference result for {label}"
        finally:
            active_gpu -= 1
            logger.info(f"GPU finished: {label}")

    @flow
    async def group(group_ctx, name):
        jobs = []
        for index in range(2):
            prepared = group_ctx.submit(prepare, f"{name}-{index}")
            jobs.append(group_ctx.submit(infer, prepared))
        # Handles in the list resolve automatically; collect receives actual results.
        return await group_ctx.submit(collect, jobs)

    # Subflows do not consume execution slots, so --slots 1 also completes.
    groups = [ctx.subflow(group, "alpha"), ctx.subflow(group, "beta")]
    results = [await group_handle for group_handle in groups]
    logger.info(f"Peak concurrent GPU tasks across both subflows: {peak_gpu}")
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slots", type=int, default=2)
    args = parser.parse_args()
    if args.slots < 1:
        parser.error("--slots must be positive")
    runtime = EventLoopRuntime(resources=TokenResourceAllocator(global_slots=args.slots, gpu=1))
    for result in runtime.run(pipeline):
        logger.info(result)


if __name__ == "__main__":
    configure_logging()
    main()
