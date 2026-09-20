"""Verify that public result types reject invalid use, even across decorators."""

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import TypedDict, cast


class Position(TypedDict):
    line: int


class DiagnosticRange(TypedDict):
    start: Position


class Diagnostic(TypedDict):
    severity: str
    rule: str
    range: DiagnosticRange
    message: str


class Report(TypedDict):
    generalDiagnostics: list[Diagnostic]


SOURCE = """\
from dataclasses import dataclass
from orchlet import EventLoopRuntime, FlowContext, SubmitOptions, agent, flow, task

@dataclass
class Rating:
    score: float

@task
async def number(value: int):
    return value + 1

@agent(backend="test", result_type=Rating)
def rate():
    return "Return a rating."

@agent(backend="test", result_type=Rating)  # reject: reportArgumentType
def invalid_prompt():
    return 123

agent(
    backend="test", result_type=Rating,
    validate=lambda rating: rating.missing,  # reject: reportAttributeAccessIssue
)

@agent  # reject: reportCallIssue
def missing_backend():
    return "Review the code."

def prompt() -> str:
    return "Return a rating."

agent()  # reject: reportCallIssue
agent(prompt)  # reject: reportCallIssue
agent(result_type=Rating)  # reject: reportCallIssue
agent(prompt, result_type=Rating)  # reject: reportCallIssue
agent(backend=None)  # reject: reportArgumentType

@flow
async def child(ctx: FlowContext, value: int):
    return await ctx.submit(number, value)

@flow
async def keyword_inputs(ctx: FlowContext, *, key: int, options: str):
    return f"{key}:{options}"

@flow
async def pipeline(ctx: FlowContext, value: int):
    count = await ctx.submit(number, value)
    count.upper()  # reject: reportAttributeAccessIssue
    rating = await ctx.submit(rate)
    rating.missing  # reject: reportAttributeAccessIssue
    rate.options(priorty=3)  # reject: reportCallIssue
    await ctx.subflow(child, "invalid")  # reject: reportArgumentType
    await ctx.subflow(child)  # reject: reportCallIssue
    await ctx.map_flows(child, ["invalid"])  # reject: reportArgumentType
    await ctx.map_flows_settled(child, ["invalid"])  # reject: reportArgumentType
    await ctx.map_flows(child, [1], key=lambda item: item)  # reject: reportArgumentType
    SubmitOptions(key=1)  # reject: reportArgumentType
    bound = ctx.with_options(SubmitOptions(key="branch"))
    await bound.subflow(child, "invalid")  # reject: reportArgumentType
    await bound.subflow(child)  # reject: reportCallIssue
    await bound.subflow(keyword_inputs, key="invalid", options="ok")  # reject: reportArgumentType
    await bound.subflow(keyword_inputs, key=1, options=3)  # reject: reportArgumentType
    bound.subflow(child, 1).missing  # reject: reportAttributeAccessIssue
    (await bound.asubmit(number, 1)).upper()  # reject: reportAttributeAccessIssue
    return rating

def check_runtime(runtime: EventLoopRuntime) -> None:
    runtime.run(pipeline, "invalid")  # reject: reportArgumentType
    runtime.run(pipeline)  # reject: reportCallIssue
    runtime.run(pipeline, 1).missing  # reject: reportAttributeAccessIssue

async def check_async_runtime(runtime: EventLoopRuntime) -> None:
    result = await runtime.start(pipeline, 1)
    result.missing  # reject: reportAttributeAccessIssue
    await runtime.arun(pipeline, "invalid")  # reject: reportArgumentType
"""


@unittest.skipUnless(importlib.util.find_spec("pyright"), "Install the dev extra to check typing")
class TypingTests(unittest.TestCase):
    def test_invalid_calls_and_result_access_are_rejected(self) -> None:
        root = Path(__file__).resolve().parents[1]
        expected = {
            (line_number, line.split("# reject: ", 1)[1])
            for line_number, line in enumerate(SOURCE.splitlines())
            if "# reject: " in line
        }
        with tempfile.TemporaryDirectory(prefix="orchlet-typing-") as directory:
            path = Path(directory) / "invalid_usage.py"
            path.write_text(SOURCE, encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pyright",
                    "--project",
                    str(root),
                    "--pythonpath",
                    sys.executable,
                    "--outputjson",
                    str(path),
                ],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        report = cast(Report, json.loads(result.stdout))
        errors = [d for d in report["generalDiagnostics"] if d["severity"] == "error"]
        actual = {(d["range"]["start"]["line"], d["rule"]) for d in errors}
        self.assertLessEqual(expected, actual, result.stdout)
        # Unknown-type follow-on errors on invalid expressions are allowed. Any
        # diagnostic elsewhere indicates a broken fixture or a resolution failure.
        self.assertLessEqual(
            {line for line, _ in actual}, {line for line, _ in expected}, result.stdout
        )
