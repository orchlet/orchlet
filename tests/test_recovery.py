"""Exercise durable boundaries using local tasks and subprocesses, without model calls."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from orchlet import EventLoopRuntime, FileArtifactStore, FlowContext, agent, flow, task
from orchlet.backends import CommandBackend
from orchlet.artifacts import read_json
from orchlet.errors import RecoveryError, TaskCancelled, TaskFailed
from orchlet.handles import TaskHandle
from orchlet.policies import ExponentialRetry


@dataclass
class SavedValue:
    path: Path
    items: list[int]


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.runtimes: list[EventLoopRuntime] = []

    async def asyncTearDown(self) -> None:
        for runtime in self.runtimes:
            await runtime.aclose()
        self.temporary.cleanup()

    def runtime(self, **kwargs: Any) -> EventLoopRuntime:
        runtime = EventLoopRuntime(artifacts=FileArtifactStore(self.directory / "runs"), **kwargs)
        self.runtimes.append(runtime)
        return runtime

    async def test_automatic_resume_preserves_types_attempts_and_dynamic_subflows(self) -> None:
        calls: list[tuple[str, int]] = []
        fail = True
        handles: list[TaskHandle[int]] = []

        @task
        def discover() -> SavedValue:
            calls.append(("discover", 0))
            return SavedValue(self.directory, [1, 2])

        @task
        async def process(value: int) -> int:
            calls.append(("process", value))
            if value == 2:
                await asyncio.sleep(0.05)
                if fail:
                    raise ValueError("Try again")
            return value * 10

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            handle = ctx.submit(process, value)
            handles.append(handle)
            return await handle

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            value = await ctx.submit(discover)
            self.assertIsInstance(value, SavedValue)
            self.assertEqual(value.path, self.directory)
            children = [ctx.subflow(branch, item) for item in value.items]
            return [await child for child in children]

        first = self.runtime().start(pipeline)
        with self.assertRaises(TaskFailed):
            await first
        fail = False
        second = self.runtime().start(pipeline)
        self.assertEqual(second.id, first.id)
        self.assertEqual(await second, [10, 20])
        self.assertEqual(calls, [("discover", 0), ("process", 1), ("process", 2), ("process", 2)])
        self.assertEqual(len(handles[-1].details.attempts if handles[-1].details else ()), 2)
        self.assertTrue((handles[-1].artifacts_dir / "attempts/001/result.json").is_file())
        self.assertTrue((handles[-1].artifacts_dir / "attempts/002/result.json").is_file())
        third = self.runtime().start(pipeline)
        self.assertNotEqual(third.id, first.id)
        self.assertEqual(await third, [10, 20])

    async def test_streams_are_live_and_survive_cancellation(self) -> None:
        backend = CommandBackend(
            [
                sys.executable,
                "-u",
                "-c",
                "import sys,time; print('partial'); print('diagnostic',file=sys.stderr); time.sleep(30)",
            ]
        )
        handles: list[TaskHandle[str]] = []

        @agent(backend=backend)
        def work() -> str:
            return "The saved prompt"

        @flow
        async def pipeline(ctx: FlowContext) -> str:
            handle = ctx.submit(work)
            handles.append(handle)
            return await handle

        run = self.runtime().start(pipeline)
        async with asyncio.timeout(4):
            while not handles:
                await asyncio.sleep(0.005)
            logs = handles[0].artifacts_dir / "attempts/001"
            while not (logs / "stdout.log").exists() or not (logs / "stdout.log").read_text():
                await asyncio.sleep(0.005)
        self.assertEqual((logs / "stdout.log").read_text(), "partial\n")
        await run.cancel()
        with self.assertRaises(TaskCancelled):
            await run
        details = handles[0].details
        assert details is not None
        self.assertEqual(details.stdout, "partial\n")
        self.assertEqual(details.stderr, "diagnostic\n")
        self.assertEqual((logs / "prompt.txt").read_text(), "The saved prompt")
        self.assertIsNotNone(details.exit_code)
        self.assertEqual(
            json.loads((run.artifacts_dir / "run.json").read_text())["status"], "interrupted"
        )

    async def test_validation_failure_retains_each_attempt_and_feedback(self) -> None:
        backend = CommandBackend(
            [
                sys.executable,
                "-c",
                "import sys; prompt=sys.stdin.read(); "
                "print('42' if 'Previous attempt error:' in prompt else 'invalid JSON'); "
                "print('trace',file=sys.stderr)",
            ]
        )
        handles: list[TaskHandle[int]] = []

        @agent(backend=backend, result_type=int, retry=ExponentialRetry(max_attempts=2, delay=0))
        def work() -> str:
            return "Return an integer"

        @flow
        async def pipeline(ctx: FlowContext) -> int:
            handle = ctx.submit(work)
            handles.append(handle)
            return await handle

        self.assertEqual(await self.runtime().arun(pipeline), 42)
        details = handles[0].details
        assert details is not None
        self.assertEqual([a.exit_code for a in details.attempts], [0, 0])
        for number in (1, 2):
            path = handles[0].artifacts_dir / f"attempts/{number:03}"
            self.assertEqual((path / "stderr.log").read_text(), "trace\n")
            self.assertTrue((path / "output.txt").is_file())
        self.assertIn("OutputValidationError", details.attempts[0].error or "")
        self.assertIn(
            "invalid JSON", (handles[0].artifacts_dir / "attempts/002/prompt.txt").read_text()
        )

    async def test_changed_submission_fails_closed_and_fresh_run_is_explicit(self) -> None:
        supplied = 1

        @task
        def work(value: int) -> int:
            return value

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            await ctx.submit(work, supplied)
            raise ValueError("Controller failure")

        with self.assertRaises(ValueError):
            await self.runtime().arun(pipeline)
        supplied = 2
        with self.assertRaises(RecoveryError):
            await self.runtime().arun(pipeline)
        with self.assertRaisesRegex(ValueError, "Controller failure"):
            await self.runtime(resume=False).arun(pipeline)

    async def test_active_run_cannot_be_opened_twice(self) -> None:
        entered = asyncio.Event()

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            entered.set()
            await asyncio.Event().wait()

        run = self.runtime().start(pipeline)
        await entered.wait()
        with self.assertRaisesRegex(RecoveryError, "already running"):
            self.runtime().start(pipeline)
        await run.cancel()
        with self.assertRaises(TaskCancelled):
            await run

    async def test_corrupted_success_checkpoint_is_not_silently_reexecuted(self) -> None:
        handles: list[TaskHandle[int]] = []
        calls = 0

        @task
        def work() -> int:
            nonlocal calls
            calls += 1
            return 7

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            handle = ctx.submit(work)
            handles.append(handle)
            await handle
            raise ValueError("Later failure")

        with self.assertRaises(ValueError):
            await self.runtime().arun(pipeline)
        (handles[0].artifacts_dir / "value.pickle").write_bytes(b"damaged")
        with self.assertRaisesRegex(RecoveryError, "damaged"):
            await self.runtime().arun(pipeline)
        self.assertEqual(calls, 1)


@unittest.skipUnless(os.name == "posix", "hard process termination requires POSIX")
class ProcessRecoveryTests(unittest.TestCase):
    def test_same_command_after_kill_resumes_batch_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "batch.py"
            script.write_text("""import asyncio
from pathlib import Path
from orchlet import EventLoopRuntime, FlowContext, flow, task

@task
async def review(value: int) -> int:
    with Path("calls").open("a") as stream:
        stream.write(f"review-{value}\\n")
    return value

@task
async def fix(value: int) -> int:
    Path(f"entered-{value}").touch()
    if not Path("continue").exists():
        await asyncio.Event().wait()
    return value + 10

@flow
async def branch(ctx: FlowContext, value: int) -> int:
    return await ctx.submit(fix, await ctx.submit(review, value))

@flow
async def pipeline(ctx: FlowContext) -> list[int]:
    return await ctx.map_flows(branch, range(3), key=str)

print(EventLoopRuntime().run(pipeline))
""")
            command = [sys.executable, str(script)]
            child = subprocess.Popen(
                command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            try:
                import time

                deadline = time.monotonic() + 8
                while not all((root / f"entered-{value}").exists() for value in range(3)):
                    if child.poll() is not None or time.monotonic() > deadline:
                        self.fail("Batch did not reach all interruption points")
                    time.sleep(0.01)
                child.kill()
                child.communicate(timeout=5)
                (root / "continue").touch()
                resumed = subprocess.run(
                    command, cwd=root, capture_output=True, text=True, timeout=10
                )
                self.assertEqual(resumed.returncode, 0, resumed.stderr)
                self.assertEqual(resumed.stdout.strip(), "[10, 11, 12]")
                self.assertEqual(
                    sorted((root / "calls").read_text().splitlines()),
                    ["review-0", "review-1", "review-2"],
                )
                runs = list((root / ".orchlet/runs").glob("*/run.json"))
                self.assertEqual(len(runs), 1)
                self.assertEqual(json.loads(runs[0].read_text())["status"], "succeeded")
                batch = read_json(next((runs[0].parent / "batches").glob("*.json")))
                self.assertEqual(batch["status"], "settled")
                self.assertEqual(set(batch["members"]), {"0", "1", "2"})
            finally:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=5)

    def test_same_command_after_kill_reuses_committed_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script = root / "pipeline.py"
            script.write_text("""import asyncio
from pathlib import Path
from orchlet import EventLoopRuntime, FlowContext, flow, task

@task
def first() -> int:
    with Path("calls").open("a") as stream:
        stream.write("first\\n")
    return 42

@task
async def second(value: int) -> int:
    Path("entered").touch()
    if not Path("continue").exists():
        await asyncio.sleep(60)
    return value + 1

@flow
async def pipeline(ctx: FlowContext) -> int:
    return await ctx.submit(second, ctx.submit(first))

print(EventLoopRuntime().run(pipeline))
""")
            command = [sys.executable, str(script)]
            child = subprocess.Popen(
                command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            try:
                import time

                deadline = time.monotonic() + 8
                while not (root / "entered").exists():
                    if child.poll() is not None or time.monotonic() > deadline:
                        self.fail("Child did not reach the interruption point")
                    time.sleep(0.01)
                child.kill()
                child.communicate(timeout=5)
                (root / "continue").touch()
                resumed = subprocess.run(
                    command, cwd=root, capture_output=True, text=True, timeout=10
                )
                self.assertEqual(resumed.returncode, 0, resumed.stderr)
                self.assertEqual(resumed.stdout.strip(), "43")
                self.assertEqual((root / "calls").read_text(), "first\n")
                runs = list((root / ".orchlet/runs").glob("*/run.json"))
                self.assertEqual(len(runs), 1)
                self.assertEqual(json.loads(runs[0].read_text())["status"], "succeeded")
            finally:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=5)
