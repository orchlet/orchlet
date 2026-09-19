"""Validate text and route to a summary or repair agent. Show both branches by default."""

import argparse

from orchlet import configure_logging, get_logger, AgentBackend, EventLoopRuntime, agent, flow
from orchlet.errors import TaskFailed
from orchlet.models import AgentReply

logger = get_logger("examples.conditional_routing")


def require_lgtm(text):
    # Require a standalone LGTM line so that "not LGTM" does not count as approval.
    if not any(line.strip().upper() == "LGTM" for line in text.splitlines()):
        raise ValueError("Review failed: no standalone LGTM line")


@agent(backend="demo", validate=require_lgtm)
def review(code):
    return f"Review this code. If approved, write LGTM on its own line; otherwise explain:\n{code}"


@agent(backend="demo")
def repair(code, feedback, error):
    return f"Repair this code:\n{code}\nReview feedback:\n{feedback}\nFailure reason: {error}"


@agent(backend="demo")
def summarize(code, review_text):
    return f"Write a description of this approved code:\n{code}\nReview result:\n{review_text}"


@flow
async def pipeline(ctx, code):
    job = ctx.submit(review, code)
    try:
        review_text = await job
    except TaskFailed as exc:
        logger.warning("Failure branch -> repair; reason: %s", exc.cause)
        # Pass the response text. A failed handle as input would skip the downstream task.
        return await ctx.submit(repair, code, job.details.raw_text or "", str(exc.cause))
    else:
        logger.info("  Success branch -> summarize")
        return await ctx.submit(summarize, code, review_text)


class DemoBackend(AgentBackend):
    """Return local demo responses without executing code or calling a model."""

    def __init__(self, case):
        self.case = case

    async def run_turn(self, request, emit, cancellation):
        name = request.task_id.rsplit(":", 1)[-1]
        if name == "review":
            text = (
                "LGTM"
                if self.case == "success"
                else "add should return a sum, but returns a difference."
            )
        elif name == "repair":
            text = "def add(a, b):\n    return a + b"
        elif name == "summarize":
            text = "add(a, b) returns the sum of a and b."
        else:
            raise ValueError(f"Unknown demo agent: {name}")
        return AgentReply(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("success", "failure", "both"), default="both")
    args = parser.parse_args()
    cases = ("success", "failure") if args.case == "both" else (args.case,)
    for case in cases:
        code = (
            "def add(a, b):\n    return a + b"
            if case == "success"
            else ("def add(a, b):\n    return a - b")
        )
        logger.info(f"Scenario: {case}")
        runtime = EventLoopRuntime(concurrency=1, backends={"demo": DemoBackend(case)})
        logger.info(runtime.run(pipeline, code))


if __name__ == "__main__":
    configure_logging()
    main()
