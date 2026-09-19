import asyncio
import json
import os
import sys
import tempfile
import unittest
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from orchlet import AgentBackend, EventLoopRuntime, FlowContext, TaskDef, TaskHandle, agent, flow
from orchlet.backends import CodexBackend, CommandBackend
from orchlet.contracts import Emit
from orchlet.errors import (
    BackendError,
    ConfigurationError,
    OutputValidationError,
    TaskCancelled,
    TaskFailed,
)
from orchlet.models import AgentReply, AgentRequest
from orchlet.policies import ExponentialRetry
from orchlet.runners import CancellationToken
from orchlet.sessions import FixedSession


@dataclass
class Rating:
    singer: str
    score: float


class ScriptedBackend(AgentBackend):
    def __init__(self, responses: Iterable[str]) -> None:
        self.responses = iter(responses)
        self.requests: list[AgentRequest] = []

    async def run_turn(
        self, request: AgentRequest, emit: Emit, cancellation: CancellationToken
    ) -> AgentReply:
        self.requests.append(request)
        return AgentReply(next(self.responses))


class AgentDefinitionTests(unittest.TestCase):
    def test_backend_is_required_for_decorators_and_direct_calls(self):
        def prompt() -> str:
            return "Return a rating."

        # Bypass static checking to verify that dynamic callers also fail immediately.
        define_agent = cast(Callable[..., object], agent)
        for function in (None, prompt):
            for structured in (False, True):
                options: dict[str, object] = {"result_type": Rating} if structured else {}
                with self.subTest(function=function, structured=structured):
                    with self.assertRaisesRegex(TypeError, "backend"):
                        define_agent(function, **options)

    def test_low_level_agent_definitions_require_a_backend(self):
        def prompt() -> str:
            return "Review the code."

        with self.assertRaisesRegex(TypeError, "backend"):
            cast(Callable[..., object], TaskDef)(function=prompt, name="review", kind="agent")
        with self.assertRaisesRegex(ValueError, "explicit backend"):
            TaskDef[[], str](function=prompt, name="review", kind="agent", backend=None)


class AgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_backend_instance_needs_no_runtime_registration(self):
        backend = ScriptedBackend(['{"singer":"A","score":8}'])

        @agent(backend=backend, result_type=Rating)
        def rate():
            return "Rate singer A."

        @flow
        async def pipeline(ctx: FlowContext):
            return await ctx.submit(rate)

        self.assertEqual(await EventLoopRuntime().arun(pipeline), Rating("A", 8.0))
        self.assertEqual(backend.requests[0].prompt, "Rate singer A.")

    async def test_structured_output_and_repair_feedback(self):
        backend = ScriptedBackend(
            ["not json", '{"singer":"A", "score":99}', '{"singer":"A", "score":8}']
        )
        handles: list[TaskHandle[Rating]] = []

        @agent(
            backend="fake",
            result_type=Rating,
            validate=lambda r: 0 <= r.score <= 10,
            retry=ExponentialRetry(max_attempts=3, delay=0),
        )
        def rate():
            return "Rate singer A."

        @flow
        async def pipeline(ctx: FlowContext):
            handle = ctx.submit(rate)
            handles.append(handle)
            return await handle

        runtime = EventLoopRuntime(backends={"fake": backend})
        result = await asyncio.wait_for(runtime.arun(pipeline), 3)
        self.assertEqual(result, Rating("A", 8.0))
        details = handles[0].details
        assert details is not None
        self.assertEqual(len(details.attempts), 3)
        self.assertIn("not json", backend.requests[1].prompt)
        self.assertIn("OutputValidationError", backend.requests[1].prompt)
        self.assertIn('"score":99', backend.requests[2].prompt)
        self.assertEqual(details.raw_text, '{"singer":"A", "score":8}')

    async def test_invalid_output_is_not_published_to_downstream(self):
        backend = ScriptedBackend(["[1,2,3]"])
        handles: list[TaskHandle[list[str]]] = []

        @agent(backend="fake", result_type=list[str])
        def generate():
            return "Names"

        @flow
        async def pipeline(ctx: FlowContext):
            handle = ctx.submit(generate)
            handles.append(handle)
            return await handle

        runtime = EventLoopRuntime(backends={"fake": backend})
        with self.assertRaises(TaskFailed) as caught:
            await runtime.arun(pipeline)
        self.assertIsInstance(caught.exception.cause, OutputValidationError)
        details = handles[0].details
        assert details is not None
        self.assertEqual(details.raw_text, "[1,2,3]")

    async def test_same_session_is_serialized(self):
        class SessionBackend(AgentBackend):
            supports_resume = True

            def __init__(self):
                self.active, self.peak = 0, 0
                self.sessions: list[str | None] = []

            async def run_turn(
                self, request: AgentRequest, emit: Emit, cancellation: CancellationToken
            ) -> AgentReply:
                self.sessions.append(request.session_id)
                self.active += 1
                self.peak = max(self.peak, self.active)
                await asyncio.sleep(0.01)
                self.active -= 1
                return AgentReply("ok", session_id=request.session_id)

        backend = SessionBackend()

        @agent(backend="fake", session=FixedSession("existing-session"))
        def prompt(value: str):
            return value

        @flow
        async def pipeline(ctx: FlowContext):
            return await ctx.all_settled([ctx.submit(prompt, "a"), ctx.submit(prompt, "b")])

        runtime = EventLoopRuntime(concurrency=2, backends={"fake": backend})
        result = await runtime.arun(pipeline)
        self.assertTrue(all(r.succeeded for r in result))
        self.assertEqual(backend.peak, 1)
        self.assertEqual(backend.sessions, ["existing-session", "existing-session"])

    async def test_session_capability_mismatch_is_explicit(self):
        @agent(backend="fake", session=FixedSession("session"))
        def prompt():
            return "hello"

        @flow
        async def pipeline(ctx: FlowContext):
            return await ctx.submit(prompt)

        with self.assertRaises(TaskFailed) as caught:
            await EventLoopRuntime(backends={"fake": ScriptedBackend(["ok"])}).arun(pipeline)
        self.assertIsInstance(caught.exception.cause, ConfigurationError)

    async def test_command_backend_uses_stdin_and_captures_stderr(self):
        backend = CommandBackend(
            [
                sys.executable,
                "-c",
                (
                    "import sys; value=sys.stdin.read(); print(value); print('diagnostic', file=sys.stderr)"
                ),
            ]
        )
        handles: list[TaskHandle[dict[str, int]]] = []

        @agent(backend="cmd", result_type=dict[str, int])
        def prompt():
            return '{"answer": 42}'

        @flow
        async def pipeline(ctx: FlowContext):
            handle = ctx.submit(prompt)
            handles.append(handle)
            return await handle

        result = await EventLoopRuntime(backends={"cmd": backend}).arun(pipeline)
        self.assertEqual(result, {"answer": 42})
        details = handles[0].details
        assert details is not None
        self.assertEqual(details.stderr, "diagnostic\n")

    async def test_command_exit_error_retains_diagnostics(self):
        backend = CommandBackend(
            [sys.executable, "-c", "import sys; print('bad',file=sys.stderr); sys.exit(2)"]
        )
        with self.assertRaises(BackendError) as caught:
            await backend.run_turn(
                AgentRequest("", "task", 1), lambda _kind, _data: None, CancellationToken()
            )
        self.assertEqual(caught.exception.exit_code, 2)
        self.assertIn("bad", caught.exception.stderr)

    async def test_subprocess_cancellation_waits_for_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pid"
            backend = CommandBackend(
                [
                    sys.executable,
                    "-c",
                    (
                        "import os,sys,time; from pathlib import Path; "
                        "Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)"
                    ),
                    str(path),
                ]
            )
            cancellation = CancellationToken()
            invocation = asyncio.create_task(
                backend.run_turn(
                    AgentRequest("", "task", 1),
                    lambda _kind, _data: None,
                    cancellation,
                )
            )
            try:
                async with asyncio.timeout(3):
                    while not path.exists():
                        await asyncio.sleep(0.005)
                pid = int(path.read_text())
                cancellation.request()
                with self.assertRaises(TaskCancelled):
                    await asyncio.wait_for(invocation, 3)
                if os.name == "posix":
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)
            finally:
                cancellation.request()
                await asyncio.gather(invocation, return_exceptions=True)

    @unittest.skipUnless(os.name == "posix", "executable fixture uses POSIX shebang")
    async def test_codex_adapter_with_fake_executable_no_model_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / "fake-codex"
            invocation_path = Path(directory) / "invocation.json"
            executable.write_text(
                f"#!{sys.executable}\n"
                "import sys,json\nfrom pathlib import Path\n"
                "prompt=sys.stdin.read()\n"
                f"Path({str(invocation_path)!r}).write_text(json.dumps([sys.argv[1:],prompt]))\n"
                "output=Path(sys.argv[sys.argv.index('--output-last-message')+1])\n"
                "output.write_text('{\"answer\":42}')\n"
                "print(json.dumps({'type':'thread.started','thread_id':'test-thread'}))\n"
            )
            executable.chmod(0o755)
            backend = CodexBackend(executable=str(executable))
            for session in (None, "existing-thread"):
                reply = await backend.run_turn(
                    AgentRequest("test prompt", "task", 1, session),
                    lambda _kind, _data: None,
                    CancellationToken(),
                )
                args, prompt = json.loads(invocation_path.read_text())
                self.assertEqual(prompt, "test prompt")
                self.assertEqual(json.loads(reply.text), {"answer": 42})
                self.assertEqual(reply.session_id, "test-thread")
                if session:
                    self.assertEqual(args[-3:], ["resume", session, "-"])
                else:
                    self.assertNotIn("resume", args)
