"""Run locally with a fake backend; pass --codex to make actual Codex calls."""

import argparse
import json
from dataclasses import dataclass

from orchlet import (
    AgentBackend,
    EventLoopRuntime,
    FlowContext,
    agent,
    configure_logging,
    flow,
    get_logger,
    task,
)
from orchlet.backends import CodexBackend
from orchlet.contracts import Emit
from orchlet.models import AgentReply, AgentRequest
from orchlet.policies import ExponentialRetry
from orchlet.runners import CancellationToken

logger = get_logger("examples.dynamic_singers")


@dataclass
class Rating:
    singer: str
    score: float
    reason: str


@agent(backend="codex", result_type=list[str], priority=30)
def generate_singers(count: int) -> str:
    return f"List {count} singers. Return only a JSON array of strings, using English names."


@agent(backend="codex", result_type=Rating, priority=10, validate=lambda r: 0 <= r.score <= 10)
def rate_singer(singer: str) -> str:
    return (
        f"Rate singer {singer}. Return a JSON object with singer, score (0 to 10), and reason. "
        "Write the response in English."
    )


@task(priority=20)
def rank(ratings: list[Rating]) -> list[Rating]:
    return sorted(ratings, key=lambda item: item.score, reverse=True)


@flow
async def singer_flow(ctx: FlowContext, count: int = 5) -> list[Rating]:
    singers = await ctx.submit(generate_singers, count)
    jobs = [ctx.submit(rate_singer, singer) for singer in singers]
    return await ctx.submit(rank, jobs)


class DemoBackend(AgentBackend):
    async def run_turn(
        self, request: AgentRequest, emit: Emit, cancellation: CancellationToken
    ) -> AgentReply:
        if request.prompt.startswith("List "):
            count = int(request.prompt.split()[1])
            return AgentReply(json.dumps([f"Singer {i + 1}" for i in range(count)]))
        singer = request.prompt.split("Rate singer ", 1)[1].split(". Return", 1)[0]
        return AgentReply(
            json.dumps({"singer": singer, "score": 8.0, "reason": "Local demo rating"})
        )


if __name__ == "__main__":
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--codex", action="store_true")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("--count must be nonnegative")
    backend = CodexBackend() if args.codex else DemoBackend()
    runtime = EventLoopRuntime(
        concurrency=4,
        backends={"codex": backend},
        retries=ExponentialRetry(max_attempts=3, delay=0.1),
    )
    for rating in runtime.run(singer_flow, count=args.count):
        logger.info(f"{rating.singer}: {rating.score} - {rating.reason}")
