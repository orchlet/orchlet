"""Behavioral checks for batch ownership, failure policies, and durable replay."""

import asyncio
from collections import Counter
from collections.abc import Callable, Iterator
from pathlib import Path
import tempfile
from typing import Any
import unittest

from orchlet import (
    BatchDecision,
    BatchFailed,
    BatchFailurePolicy,
    BatchSnapshot,
    EventLoopRuntime,
    FailFast,
    FileArtifactStore,
    FlowContext,
    FlowHandle,
    Outcome,
    flow,
    task,
)
from orchlet.artifacts import read_json
from orchlet.errors import RecoveryError, TaskCancelled, TaskFailed
from orchlet.policies import ExponentialRetry
from orchlet.resources import TokenResourceAllocator


async def until(predicate: Callable[[], bool]) -> None:
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.001)


class BatchTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.runtimes: list[EventLoopRuntime] = []
        self.loop_errors: list[dict[str, Any]] = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: self.loop_errors.append(context)
        )

    def runtime(self, **kwargs: Any) -> EventLoopRuntime:
        runtime = EventLoopRuntime(artifacts=FileArtifactStore(self.directory / "runs"), **kwargs)
        self.runtimes.append(runtime)
        return runtime

    async def asyncTearDown(self) -> None:
        for runtime in self.runtimes:
            await asyncio.wait_for(runtime.aclose(), 5)
        await asyncio.sleep(0)
        self.temporary.cleanup()
        self.assertEqual(self.loop_errors, [], "Batch leaked an asynchronous error")

    async def test_all_settled_accepts_mixed_task_and_flow_handles(self) -> None:
        @task
        def number() -> int:
            return 42

        @flow
        async def child(ctx: FlowContext, fail: bool) -> bool:
            if fail:
                raise ValueError("review failed")
            return False

        @flow
        async def pipeline(ctx: FlowContext) -> list[Outcome[int | bool]]:
            return await ctx.all_settled(
                [ctx.submit(number), ctx.subflow(child, False), ctx.subflow(child, True)]
            )

        outcomes = await self.runtime().arun(pipeline)
        self.assertEqual([outcome.succeeded for outcome in outcomes], [True, True, False])
        self.assertEqual([outcome.value for outcome in outcomes[:2]], [42, False])
        self.assertIsInstance(outcomes[2].error, ValueError)
        self.assertEqual(outcomes[2].node_id, outcomes[2].task_id)

    async def test_all_settled_rejects_cross_run_and_cross_runtime_handles(self) -> None:
        handles: list[FlowHandle[int]] = []

        @flow
        async def child(ctx: FlowContext) -> int:
            return 1

        @flow
        async def producer(ctx: FlowContext) -> int:
            handle = ctx.subflow(child)
            handles.append(handle)
            return await handle

        @flow
        async def consumer(ctx: FlowContext) -> None:
            await ctx.all_settled(handles)

        owner = self.runtime()
        await owner.arun(producer)
        for runtime in (owner, self.runtime()):
            with self.assertRaisesRegex(ValueError, "Runtime and run"):
                await runtime.arun(consumer)

    async def test_wait_all_collects_original_failures_and_finishes_other_members(self) -> None:
        completed: list[int] = []
        errors = {1: ValueError("one"), 3: RuntimeError("three")}

        @task
        async def work(value: int) -> int:
            if value in errors:
                raise errors[value]
            await asyncio.sleep(0.01)
            completed.append(value)
            return value

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            return await ctx.submit(work, value)

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            return await ctx.map_flows(branch, range(6), key=str, max_in_flight=2)

        run = self.runtime(concurrency=2).start(pipeline)
        with self.assertRaises(BatchFailed) as caught:
            await asyncio.wait_for(run.wait(), 5)
        self.assertEqual(set(completed), {0, 2, 4, 5})
        self.assertEqual({failure.key for failure in caught.exception.failures}, {"1", "3"})
        for failure in caught.exception.failures:
            self.assertIsInstance(failure.error, TaskFailed)
            self.assertIs(failure.error.__cause__, errors[int(failure.key)])
            self.assertIsNotNone(failure.task_id)
        self.assertIsInstance(caught.exception.__cause__, ExceptionGroup)
        metadata = read_json(next((run.artifacts_dir / "batches").glob("*.json")))
        self.assertEqual(metadata["status"], "failed")
        self.assertEqual(
            metadata["members"]["1"]["error"]["causes"][-1]["type"], "builtins.ValueError"
        )
        self.assertEqual(read_json(run.artifacts_dir / "run.json")["status"], "failed")

    async def test_settled_handles_empty_none_false_and_errors(self) -> None:
        @flow
        async def branch(ctx: FlowContext, value: int) -> bool | None:
            if value == 2:
                raise ValueError("failed")
            return False if value == 0 else None

        @flow
        async def pipeline(ctx: FlowContext) -> list[Outcome[bool | None]]:
            self.assertEqual(await ctx.map_flows(branch, []), [])
            self.assertEqual(await ctx.map_flows_settled(branch, []), [])
            self.assertEqual(await ctx.map_flows(branch, [0, 1]), [False, None])
            return await ctx.map_flows_settled(branch, range(3), key=str, max_in_flight=1)

        run = self.runtime().start(pipeline)
        outcomes = await run
        self.assertEqual([outcome.succeeded for outcome in outcomes], [True, True, False])
        self.assertEqual([outcome.key for outcome in outcomes], ["0", "1", "2"])
        self.assertEqual(read_json(run.artifacts_dir / "run.json")["status"], "succeeded")

    async def test_default_has_no_branch_window(self) -> None:
        started: list[int] = []
        release = asyncio.Event()

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            started.append(value)
            await release.wait()
            return value

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            return await ctx.map_flows(branch, range(40))

        run = self.runtime(concurrency=1).start(pipeline)
        await until(lambda: len(started) == 40)
        release.set()
        self.assertEqual(await run, list(range(40)))

    async def test_window_rolls_after_whole_branch_without_review_barrier(self) -> None:
        admitted: list[int] = []
        first_review = asyncio.Event()
        second_fix = asyncio.Event()
        finish_fix = asyncio.Event()

        def items() -> Iterator[int]:
            for value in range(3):
                admitted.append(value)
                yield value

        @task
        async def review(value: int) -> int:
            if value == 0:
                await first_review.wait()
            return value

        @task
        async def fix(value: int) -> int:
            if value == 1:
                second_fix.set()
                await finish_fix.wait()
            return value

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            return await ctx.submit(fix, await ctx.submit(review, value))

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            return await ctx.map_flows(branch, items(), max_in_flight=2)

        run = self.runtime(concurrency=2).start(pipeline)
        await asyncio.wait_for(second_fix.wait(), 5)
        self.assertEqual(admitted, [0, 1])
        self.assertFalse(first_review.is_set())
        finish_fix.set()
        await until(lambda: admitted == [0, 1, 2])
        first_review.set()
        self.assertEqual(await run, [0, 1, 2])

    async def test_fail_fast_waits_for_cleanup_and_preserves_unrelated_work(self) -> None:
        started = asyncio.Event()
        cleaning = asyncio.Event()
        release_cleanup = asyncio.Event()
        consumed: list[int] = []
        failures: list[BatchFailed] = []

        def items() -> Iterator[int]:
            for value in range(5):
                consumed.append(value)
                yield value

        @task
        async def work(value: int) -> int:
            if value == 0:
                await started.wait()
                raise ValueError("stop batch")
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release_cleanup.wait()
            return value

        @task
        async def unrelated() -> int:
            await cleaning.wait()
            return 42

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            return await ctx.submit(work, value)

        @flow
        async def pipeline(ctx: FlowContext) -> int:
            other = ctx.submit(unrelated)
            try:
                await ctx.map_flows(branch, items(), max_in_flight=2, failure=FailFast())
            except BatchFailed as error:
                failures.append(error)
            return await other

        run = self.runtime(concurrency=3).start(pipeline)
        await asyncio.wait_for(cleaning.wait(), 5)
        self.assertFalse(run.done)
        self.assertEqual(failures, [])
        self.assertEqual(consumed, [0, 1])
        release_cleanup.set()
        self.assertEqual(await run, 42)
        self.assertEqual([failure.key for failure in failures[0].failures], ["0"])

    async def test_parent_cancellation_stops_submission_and_waits_for_cleanup(self) -> None:
        started = asyncio.Event()
        cleaning = asyncio.Event()
        release = asyncio.Event()
        consumed: list[int] = []

        def items() -> Iterator[int]:
            for value in range(5):
                consumed.append(value)
                yield value

        @task
        async def work(value: int) -> int:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()
            return value

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            return await ctx.submit(work, value)

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            return await ctx.map_flows(branch, items(), max_in_flight=1)

        run = self.runtime().start(pipeline)
        await asyncio.wait_for(started.wait(), 5)
        await run.cancel()
        await asyncio.wait_for(cleaning.wait(), 5)
        self.assertFalse(run.done)
        release.set()
        with self.assertRaises(TaskCancelled):
            await asyncio.wait_for(run.wait(), 5)
        self.assertEqual(consumed, [0])
        metadata = read_json(next((run.artifacts_dir / "batches").glob("*.json")))
        self.assertEqual(metadata["status"], "cancelled")
        self.assertEqual(metadata["members"]["0"]["status"], "failed")

    async def test_flow_handle_can_cancel_descendants(self) -> None:
        started = asyncio.Event()
        stopped = asyncio.Event()

        @task
        async def work() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        @flow
        async def branch(ctx: FlowContext) -> None:
            await ctx.submit(work)

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            child = ctx.subflow(branch)
            self.assertEqual(child.run_id, ctx.run_id)
            await started.wait()
            self.assertFalse(child.done)
            await child.cancel()
            outcomes = await ctx.all_settled([child])
            self.assertIsInstance(outcomes[0].error, TaskCancelled)
            self.assertTrue(child.done)
            self.assertTrue(stopped.is_set())
            await child.cancel()

        await self.runtime().arun(pipeline)

    async def test_invalid_keys_and_iterator_failure_cancel_members(self) -> None:
        stopped = asyncio.Event()
        started = asyncio.Event()

        @task
        async def work() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        @flow
        async def branch(ctx: FlowContext, value: int) -> None:
            await ctx.submit(work)

        def items() -> Iterator[int]:
            yield 0
            raise ValueError("broken iterator")

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            with self.assertRaisesRegex(ValueError, "broken iterator"):
                await ctx.map_flows(branch, items())
            if started.is_set():
                self.assertTrue(stopped.is_set())
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                await ctx.map_flows(branch, [0, 1], key=lambda _: "same")
            with self.assertRaisesRegex(ValueError, "nonempty"):
                await ctx.map_flows(branch, [0], key=lambda _: "")
            for limit in (0, -1, True):
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    await ctx.map_flows(branch, [], max_in_flight=limit)

        await asyncio.wait_for(self.runtime().arun(pipeline), 5)

    async def test_leaf_retry_finishes_before_fail_fast_decides(self) -> None:
        attempts: Counter[int] = Counter()

        @task(retry=ExponentialRetry(max_attempts=2, delay=0))
        async def work(value: int) -> int:
            attempts[value] += 1
            if attempts[value] == 1:
                raise ValueError("retry")
            return value

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            return await ctx.submit(work, value)

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            return await ctx.map_flows(branch, range(3), failure=FailFast())

        self.assertEqual(await self.runtime().arun(pipeline), [0, 1, 2])
        self.assertEqual(attempts, {0: 2, 1: 2, 2: 2})

    async def test_custom_failure_policy_can_stop_admission(self) -> None:
        class StopOnFailure(BatchFailurePolicy):
            def decide(self, snapshot: BatchSnapshot) -> BatchDecision:
                return BatchDecision.STOP if snapshot.failures else BatchDecision.CONTINUE

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            raise ValueError(str(value))

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            return await ctx.map_flows(branch, range(4), max_in_flight=1, failure=StopOnFailure())

        # Locally defined strategies need not be pickleable just to run a batch.
        with self.assertRaises(BatchFailed) as caught:
            await self.runtime().arun(pipeline)
        self.assertEqual(len(caught.exception.failures), 1)

    async def test_resource_limits_still_control_leaf_dispatch(self) -> None:
        active: Counter[str] = Counter()
        peak: Counter[str] = Counter()

        async def step(role: str) -> None:
            active[role] += 1
            active["total"] += 1
            peak[role] = max(peak[role], active[role])
            peak["total"] = max(peak["total"], active["total"])
            try:
                await asyncio.sleep(0.02)
            finally:
                active[role] -= 1
                active["total"] -= 1

        @task(resources={"reviews": 1})
        async def review(value: int) -> bool:
            await step("reviews")
            return value % 2 == 0

        @task(resources={"fixes": 1})
        async def fix(value: int) -> int:
            await step("fixes")
            return value

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            if await ctx.submit(review, value):
                return await ctx.submit(fix, value)
            return value

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            return await ctx.map_flows(branch, range(24))

        runtime = self.runtime(
            resources=TokenResourceAllocator(global_slots=16, reviews=8, fixes=8)
        )
        self.assertEqual(await runtime.arun(pipeline), list(range(24)))
        self.assertLessEqual(peak["reviews"], 8)
        self.assertLessEqual(peak["fixes"], 8)
        self.assertLessEqual(peak["total"], 16)
        self.assertEqual(active, {"reviews": 0, "fixes": 0, "total": 0})

    async def test_keyed_recovery_reuses_reviews_and_successful_fixes(self) -> None:
        fail = True
        order = [0, 1, 2]
        calls: Counter[tuple[str, int]] = Counter()

        @task
        async def review(value: int) -> bool:
            calls["review", value] += 1
            return value != 2

        @task
        async def fix(value: int) -> str:
            calls["fix", value] += 1
            if value == 1 and fail:
                raise ValueError("fix failed")
            return f"fixed-{value}"

        @flow
        async def branch(ctx: FlowContext, value: int) -> str:
            if await ctx.submit(review, value):
                return await ctx.submit(fix, value)
            return "not-a-bug"

        @flow
        async def pipeline(ctx: FlowContext) -> list[str]:
            return await ctx.map_flows(branch, order, key=lambda value: f"bug/{value}:item")

        first = self.runtime().start(pipeline)
        with self.assertRaises(BatchFailed):
            await first
        fail = False
        order.reverse()
        second = self.runtime().start(pipeline)
        self.assertEqual(second.id, first.id)
        self.assertEqual(await second, ["not-a-bug", "fixed-1", "fixed-0"])
        self.assertEqual(
            calls,
            {("review", 0): 1, ("review", 1): 1, ("review", 2): 1, ("fix", 0): 1, ("fix", 1): 2},
        )
        metadata = read_json(next((second.artifacts_dir / "batches").glob("*.json")))
        self.assertEqual(metadata["status"], "settled")
        self.assertTrue(metadata["manifest_complete"])

    async def test_recovery_rejects_missing_and_new_members(self) -> None:
        for replacement in ([1], [0, 1, 2]):
            with self.subTest(replacement=replacement):
                order = [0, 1]
                entered: list[int] = []

                @flow
                async def branch(ctx: FlowContext, value: int) -> int:
                    entered.append(value)
                    if value == 1:
                        raise ValueError("failed")
                    return value

                @flow
                async def pipeline(ctx: FlowContext) -> list[int]:
                    return await ctx.map_flows(branch, order, key=str)

                key = str(replacement)
                with self.assertRaises(BatchFailed):
                    await self.runtime(run_key=key).arun(pipeline)
                order[:] = replacement
                with self.assertRaises(RecoveryError):
                    await asyncio.wait_for(self.runtime(run_key=key).arun(pipeline), 5)
                self.assertNotIn(2, entered)
