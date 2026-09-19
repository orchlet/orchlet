"""Static regression checks, analyzed by Pyright/Pylance rather than executed.

Keep results inferred at each call site: annotating them would hide regressions
where a decorator or orchestration method accidentally returns Any.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Any, Literal, assert_type

from orchlet import (
    EventLoopRuntime,
    FlowContext,
    FlowController,
    FlowDef,
    FlowHandle,
    RunHandle,
    Runtime,
    TaskHandle,
    agent,
    flow,
    task,
)
from orchlet.models import AttemptContext, Outcome, TaskResult
from orchlet.outputs import TypeValidator
from orchlet.runners import TaskContext


@dataclass
class Rating:
    singer: str
    score: float


@task
def increment(value: int):
    return value + 1


@task(priority=2)
async def format_number(value: int):
    return str(value)


@task(context=True)
def report(ctx: TaskContext, value: int):
    ctx.report(value=value)
    return value


@task(context=True, priority=1)
async def async_report(ctx: TaskContext, value: int):
    ctx.report(value=value)
    return str(value)


@task(result_type=Rating)
def parse_rating():
    return {"singer": "A", "score": 8.5}


@task(context=True, result_type=list[int])
def parse_values(ctx: TaskContext):
    ctx.report(value=1)
    return [1, 2]


@agent
def review(value: str):
    return f"Review {value}."


@agent(validate=lambda text: "LGTM" in assert_type(text, str))
async def async_review(value: str):
    return f"Review {value}."


@agent(result_type=Rating, validate=lambda rating: assert_type(rating, Rating).score >= 0)
def rate(singer: str):
    return f"Rate {singer}."


@agent(result_type=list[str])
def names():
    return "Return singer names."


@agent(result_type=dict[str, int])
def counts():
    return "Return counts."


@agent(result_type=Rating | None)
def optional_rating():
    return "Return a rating or null."


@agent(result_type=Literal["pass", "fail"])
def verdict():
    return "Return pass or fail."


@agent(result_type=Annotated[int, "score"])
def score():
    return "Return a score."


@flow
async def child(ctx: FlowContext, value: int):
    return await ctx.submit(format_number, value)


async def undecorated_child(ctx: FlowContext, value: int):
    return await ctx.submit(increment, value)


class CustomController(FlowController[int]):
    async def run(
        self, context: FlowContext, args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> int:
        return await context.submit(increment, 1)


@flow
async def pipeline(ctx: FlowContext, value: int):
    number = ctx.submit(increment, value)
    assert_type(number, TaskHandle[int])
    assert_type(await number, int)
    assert_type(await number.result(), int)
    assert_type(number.details, TaskResult[int] | None)
    details = number.details
    if details is not None:
        assert_type(details.value, int | None)

    assert_type(ctx.submit(increment.options(priority=10), value), TaskHandle[int])
    assert_type(await ctx.asubmit(format_number, value), TaskHandle[str])
    assert_type(await ctx.map(increment, range(3)), list[int])
    assert_type(await ctx.all_settled([number]), list[Outcome[int]])
    assert_type(
        await ctx.all_settled([number, ctx.submit(review, "code")]), list[Outcome[int | str]]
    )
    assert_type(ctx.submit(report, value), TaskHandle[int])
    assert_type(ctx.submit(async_report, value), TaskHandle[str])
    assert_type(ctx.submit(parse_rating), TaskHandle[Rating])
    assert_type(ctx.submit(parse_values), TaskHandle[list[int]])

    # Nested handles are resolved by the input resolver before execution.
    assert_type(ctx.submit(format_number, number), TaskHandle[str])
    assert_type(ctx.submit(names), TaskHandle[list[str]])
    assert_type(ctx.submit(rate.options(priority=3), "A"), TaskHandle[Rating])
    assert_type(ctx.submit(review, "code"), TaskHandle[str])
    assert_type(ctx.submit(async_review, "code"), TaskHandle[str])
    assert_type(ctx.submit(counts), TaskHandle[dict[str, int]])
    assert_type(ctx.submit(optional_rating), TaskHandle[Rating | None])
    assert_type(ctx.submit(verdict), TaskHandle[Literal["pass", "fail"]])
    assert_type(ctx.submit(score), TaskHandle[int])

    assert_type(ctx.subflow(child, value), FlowHandle[str])
    assert_type(await ctx.subflow(child, value), str)
    assert_type(await ctx.subflow(undecorated_child, value), int)
    custom = FlowDef[[], int](CustomController(), "custom")
    assert_type(await ctx.subflow(custom), int)
    return await ctx.submit(rate, "A")


def synchronous_api(runtime: EventLoopRuntime, abstraction: Runtime) -> None:
    assert_type(runtime.run(pipeline, 1), Rating)
    assert_type(abstraction.run(pipeline, 1), Rating)
    assert_type(runtime.run(undecorated_child, 1), int)


async def asynchronous_api(runtime: EventLoopRuntime, abstraction: Runtime) -> None:
    assert_type(await runtime.arun(pipeline, 1), Rating)
    assert_type(await abstraction.arun(pipeline, 1), Rating)
    assert_type(await runtime.arun(undecorated_child, 1), int)
    run = runtime.start(pipeline, 1, keep_open=True)
    assert_type(run, RunHandle[Rating])
    assert_type(await run, Rating)
    assert_type(await run.wait(), Rating)
    assert_type(run.submit(increment, 1), TaskHandle[int])
    assert_type(abstraction.start(pipeline, 1), RunHandle[Rating])
    assert_type(runtime.start(undecorated_child, 1), RunHandle[int])


def validator_types(context: AttemptContext) -> None:
    assert_type(TypeValidator(list[str]).validate(["A"], context), list[str])
    assert_type(TypeValidator(Rating | None).validate(None, context), Rating | None)
    assert_type(TypeValidator(Annotated[int, "score"]).validate(1, context), int)
