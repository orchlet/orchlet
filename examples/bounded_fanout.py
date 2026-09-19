"""Discover work dynamically and combine a map window with admission backpressure."""

import argparse
import asyncio

from orchlet import configure_logging, get_logger, EventLoopRuntime, flow, task
from orchlet.policies import BoundedAdmission

logger = get_logger("examples.bounded_fanout")


@task
def discover(count):
    # Discover items at runtime; the result determines how many tasks are needed.
    return [number for number in range(count) if number % 4 != 0]


@flow
async def pipeline(ctx, count, window):
    items = await ctx.submit(discover, count)
    logger.info(f"Discovered {len(items)} items: {items}")
    active, peak = 0, 0
    completed = []

    @task(context=True)
    async def process(task_ctx, number):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        task_ctx.report(percent=0)
        try:
            # Durations vary, but the returned results preserve input order.
            await asyncio.sleep(0.005 * (3 - number % 3))
            task_ctx.report(percent=100)
            completed.append(number)
            return number * number
        finally:
            active -= 1

    values = await ctx.map(process, items, max_in_flight=window)
    logger.info(f"Completion order: {completed}")
    logger.info(f"Results in input order: {values}")
    logger.info(f"Peak concurrent process tasks: {peak}")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=12)
    parser.add_argument("--window", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--max-pending", type=int, default=2)
    args = parser.parse_args()
    if args.count < 0:
        parser.error("--count must be nonnegative")
    if min(args.window, args.concurrency, args.max_pending) < 1:
        parser.error("--window, --concurrency and --max-pending must be positive")
    logger.info(
        f"Execution slots={args.concurrency}, admission limit={args.max_pending}, map window={args.window}"
    )
    runtime = EventLoopRuntime(
        concurrency=args.concurrency,
        admission=BoundedAdmission(max_pending=args.max_pending),
    )
    runtime.run(pipeline, args.count, args.window)


if __name__ == "__main__":
    configure_logging()
    main()
