"""Create repair and review nodes dynamically, with a limit on review rounds."""

import argparse
from dataclasses import dataclass

from orchlet import configure_logging, get_logger, AgentBackend, EventLoopRuntime, agent, flow
from orchlet.errors import TaskFailed
from orchlet.models import AgentReply

logger = get_logger("examples.review_repair_loop")


def require_lgtm(text):
    if text.strip().upper() != "LGTM":
        raise ValueError("Review failed: the code needs changes")


@agent(backend="demo", validate=require_lgtm)
def review(code):
    return f"Review the add function. Reply with only LGTM if approved, or explain the changes:\n{code}"


@agent(backend="demo")
def repair(code, feedback):
    return f"Fix the code using the review feedback. Return only code.\nCode:\n{code}\nFeedback:\n{feedback}"


@dataclass
class ReviewResult:
    approved: bool
    rounds: int
    code: str
    feedback: str


@flow
async def review_until_approved(ctx, code, max_rounds):
    for round_number in range(1, max_rounds + 1):
        job = ctx.submit(review, code)
        try:
            feedback = await job
        except TaskFailed as exc:
            feedback = job.details.raw_text or str(exc.cause)
            logger.warning("Review round %s failed: %s", round_number, feedback)
            if round_number == max_rounds:
                return ReviewResult(False, round_number, code, feedback)
            # Each round creates fresh nodes and passes feedback explicitly between sessions.
            code = await ctx.submit(repair, code, feedback)
            logger.info("  Repair received; starting another review")
        else:
            logger.info(f"Review round {round_number} approved")
            return ReviewResult(True, round_number, code, feedback)


class DemoBackend(AgentBackend):
    def __init__(self, stubborn=False):
        self.stubborn = stubborn

    async def run_turn(self, request, emit, cancellation):
        name = request.task_id.rsplit(":", 1)[-1]
        if name == "review":
            text = (
                "LGTM" if "return a + b" in request.prompt else "Replace subtraction with addition."
            )
        elif name == "repair":
            operator = "-" if self.stubborn else "+"
            text = f"def add(a, b):\n    return a {operator} b"
        else:
            raise ValueError(f"Unknown demo agent: {name}")
        return AgentReply(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-rounds", type=int, default=3)
    parser.add_argument(
        "--stubborn",
        action="store_true",
        help="Simulate ineffective repairs to reach the round limit",
    )
    args = parser.parse_args()
    if args.max_rounds < 1:
        parser.error("--max-rounds must be positive")
    runtime = EventLoopRuntime(
        concurrency=1, backends={"demo": DemoBackend(stubborn=args.stubborn)}
    )
    result = runtime.run(review_until_approved, "def add(a, b):\n    return a - b", args.max_rounds)
    status = "approved" if result.approved else "round limit reached; still rejected"
    logger.info(f"Final status: {status}; rounds: {result.rounds}")
    logger.info(result.code)


if __name__ == "__main__":
    configure_logging()
    main()
