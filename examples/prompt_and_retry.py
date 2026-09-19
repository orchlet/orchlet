"""Replace PromptBuilder to include previous responses and errors in repair prompts."""

from dataclasses import dataclass
import json

from orchlet import (
    configure_logging,
    get_logger,
    AgentBackend,
    EventLoopRuntime,
    PromptBuilder,
    agent,
    flow,
)
from orchlet.models import AgentReply
from orchlet.policies import ExponentialRetry
from orchlet.prompts import FunctionPromptBuilder

logger = get_logger("examples.prompt_and_retry")


class RepairPromptBuilder(PromptBuilder):
    def __init__(self):
        # Reuse Python prompt function invocation and define the feedback format below.
        self.base = FunctionPromptBuilder(include_feedback=False)

    async def build(self, function, args, kwargs, attempt):
        requirement = await self.base.build(function, args, kwargs, attempt)
        if attempt.previous_error is None:
            return requirement
        return (
            f"This is attempt {attempt.number}. Correct the previous response.\n"
            f"Requirements:\n{requirement}\n"
            f"Previous response:\n{attempt.previous_output or ''}\n"
            f"Error:\n{attempt.previous_error}\n"
            "Return only the corrected JSON, without Markdown code fences."
        )


@dataclass
class Rating:
    singer: str
    score: float


def check_score(rating):
    if not 0 <= rating.score <= 10:
        raise ValueError(f"score must be between 0 and 10; received {rating.score}")


@agent(
    backend="demo",
    result_type=Rating,
    validate=check_score,
    prompt_builder=RepairPromptBuilder(),
    retry=ExponentialRetry(max_attempts=3, delay=0.01),
)
def rate(singer, detailed=False):
    criteria = (
        ["vocal ability", "songs", "stage performance"] if detailed else ["overall performance"]
    )
    return (
        f"Rate {singer} based on {', '.join(criteria)}. "
        'Return only a JSON object with a "singer" string and a "score" between 0 and 10.'
    )


@flow
async def pipeline(ctx):
    job = ctx.submit(rate, "Singer A", detailed=True)
    rating = await job
    logger.info(f"Final {type(rating).__name__} object: {rating}")
    for attempt in job.details.attempts:
        logger.info(f"  Attempt {attempt.number}: {attempt.error or 'succeeded'}")
    return rating


class DemoBackend(AgentBackend):
    async def run_turn(self, request, emit, cancellation):
        responses = [
            "This response is not JSON",
            json.dumps({"singer": "Singer A", "score": 99}),
            json.dumps({"singer": "Singer A", "score": 8.5}),
        ]
        logger.info(f"\nPrompt sent to the agent for attempt {request.attempt}:\n{request.prompt}")
        return AgentReply(responses[request.attempt - 1])


if __name__ == "__main__":
    configure_logging()
    EventLoopRuntime(backends={"demo": DemoBackend()}).run(pipeline)
