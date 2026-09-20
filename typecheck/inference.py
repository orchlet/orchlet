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
    SubmitOptions,
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


@agent(backend="test")
def review(value: str):
    return f"Review {value}."


@agent(backend="test", validate=lambda text: "LGTM" in assert_type(text, str))
async def async_review(value: str):
    return f"Review {value}."


@agent(
    backend="test",
    result_type=Rating,
    validate=lambda rating: assert_type(rating, Rating).score >= 0,
)
def rate(singer: str):
    return f"Rate {singer}."


@agent(backend="test", result_type=list[str])
def names():
    return "Return singer names."


@agent(backend="test", result_type=dict[str, int])
def counts():
    return "Return counts."


@agent(backend="test", result_type=Rating | None)
def optional_rating():
    return "Return a rating or null."


@agent(backend="test", result_type=Literal["pass", "fail"])
def verdict():
    return "Return pass or fail."


@agent(backend="test", result_type=Annotated[int, "score"])
def score():
    return "Return a score."


@flow
async def child(ctx: FlowContext, value: int):
    return await ctx.submit(format_number, value)


async def undecorated_child(ctx: FlowContext, value: int):
    return await ctx.submit(increment, value)


@flow
async def keyword_inputs(ctx: FlowContext, *, key: int, options: str):
    return f"{key}:{options}"


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
    bound = ctx.with_options(SubmitOptions(key="child"))
    assert_type(bound, FlowContext)
    assert_type(bound.subflow(child, value), FlowHandle[str])
    assert_type(await bound.subflow(undecorated_child, value), int)
    assert_type(await bound.subflow(keyword_inputs, key=1, options="business"), str)
    assert_type(bound.submit(increment, value), TaskHandle[int])
    assert_type(await bound.asubmit(format_number, value), TaskHandle[str])
    assert_type(ctx.submit(increment, value, options=SubmitOptions(key="work")), TaskHandle[int])
    assert_type(number.key, str | None)
    child_handle = ctx.subflow(child, value)
    assert_type(child_handle.done, bool)
    assert_type(child_handle.run_id, str)
    assert_type(child_handle.key, str | None)
    assert_type(await ctx.all_settled([child_handle]), list[Outcome[str]])
    assert_type(await ctx.all_settled([number, child_handle]), list[Outcome[int | str]])
    assert_type(await ctx.map_flows(child, [1, 2]), list[str])
    assert_type(await ctx.map_flows(undecorated_child, [1, 2]), list[int])
    assert_type(
        await ctx.map_flows(child, [1, 2], key=lambda item: str(assert_type(item, int))),
        list[str],
    )
    assert_type(await ctx.map_flows_settled(child, [1, 2]), list[Outcome[str]])
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
    assert_type(run.submit(increment, 1, options=SubmitOptions(key="external")), TaskHandle[int])
    assert_type(abstraction.start(pipeline, 1), RunHandle[Rating])
    assert_type(runtime.start(undecorated_child, 1), RunHandle[int])


def validator_types(context: AttemptContext) -> None:
    assert_type(TypeValidator(list[str]).validate(["A"], context), list[str])
    assert_type(TypeValidator(Rating | None).validate(None, context), Rating | None)
    assert_type(TypeValidator(Annotated[int, "score"]).validate(1, context), int)
