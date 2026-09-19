import asyncio
from dataclasses import dataclass
import threading
import unittest

from orchlet import EventLoopRuntime, Scheduler, SubmitOptions, flow, task
from orchlet.clocks import VirtualClock
from orchlet.errors import (
    AdmissionError,
    DependencyFailed,
    RunClosedError,
    SchedulingError,
    TaskCancelled,
    TaskFailed,
)
from orchlet.models import ScheduleDecision, Start, TaskState
from orchlet.policies import AllSettled, AnySuccessful, BoundedAdmission, ExponentialRetry
from orchlet.schedulers import PartialOrderScheduler, WeightedScheduler
from orchlet.weights import MetricWeight


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.001)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtimes = []
        self.loop_errors = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: self.loop_errors.append(context)
        )

    def runtime(self, **kwargs):
        runtime = EventLoopRuntime(**kwargs)
        self.runtimes.append(runtime)
        return runtime

    async def asyncTearDown(self):
        for runtime in self.runtimes:
            await asyncio.wait_for(runtime.aclose(), 4)
        await asyncio.sleep(0)
        self.assertEqual(self.loop_errors, [], "Runtime leaked an asynchronous error")

    async def test_dynamic_fanout_including_empty(self):
        @task
        def generate(count):
            return list(range(count))

        @task
        def double(value):
            return value * 2

        @task
        def collect(values):
            return values

        @flow
        async def pipeline(ctx, count):
            values = await ctx.submit(generate, count)
            handles = [ctx.submit(double, v) for v in values]
            return await ctx.submit(collect, handles)

        runtime = self.runtime(concurrency=1)
        for count in (0, 1, 7):
            with self.subTest(count=count):
                result = await asyncio.wait_for(runtime.arun(pipeline, count), 3)
                self.assertEqual(result, [v * 2 for v in range(count)])

    async def test_nested_inputs_and_container_capture(self):
        @dataclass
        class Payload:
            value: object

        @task
        def source():
            return 7

        @task
        def consume(data):
            return data["items"][0].value

        @flow
        async def pipeline(ctx):
            value = ctx.submit(source)
            data = {"items": [Payload(value)]}
            handle = ctx.submit(consume, data)
            data["items"].clear()
            return await handle

        self.assertEqual(await self.runtime().arun(pipeline), 7)

    async def test_static_priority_and_stable_fifo(self):
        order = []

        @task
        async def work(label):
            order.append(label)

        @flow
        async def pipeline(ctx):
            handles = [
                ctx.submit(work, "first"),
                ctx.submit(work, "second"),
                ctx.submit(work.options(priority=100), "urgent"),
            ]
            await ctx.all_settled(handles)

        await self.runtime(concurrency=1).arun(pipeline)
        self.assertEqual(order, ["urgent", "first", "second"])

    async def test_live_weight_changes_waiting_order(self):
        started, release = asyncio.Event(), asyncio.Event()
        order, handles = [], {}

        @task(priority=1000)
        async def blocker():
            started.set()
            await release.wait()

        @task
        async def work(label):
            order.append(label)

        @flow
        async def pipeline(ctx):
            block = ctx.submit(blocker)
            first = ctx.submit(work.options(priority=10), "a")
            second = ctx.submit(work, "b")
            handles["b"] = second
            await ctx.all_settled([block, first, second])

        runtime = self.runtime(
            concurrency=1, scheduler=WeightedScheduler(weights=MetricWeight("urgency"))
        )
        run = runtime.start(pipeline)
        await asyncio.wait_for(started.wait(), 2)
        await handles["b"].update_metrics(urgency=100)
        release.set()
        await asyncio.wait_for(run.wait(), 2)
        self.assertEqual(order, ["b", "a"])

    async def test_external_submission_runs_while_another_node_is_active(self):
        started, release = asyncio.Event(), asyncio.Event()

        @task
        async def slow():
            started.set()
            await release.wait()
            return "root"

        @task(priority=100)
        async def urgent():
            return "urgent"

        @flow
        async def pipeline(ctx):
            return await ctx.submit(slow)

        run = self.runtime(concurrency=2).start(pipeline, keep_open=True)
        await asyncio.wait_for(started.wait(), 2)
        self.assertEqual(await asyncio.wait_for(run.submit(urgent).result(), 2), "urgent")
        self.assertFalse(run.done)
        release.set()
        await run.close_inputs()
        self.assertEqual(await asyncio.wait_for(run.wait(), 2), "root")
        with self.assertRaises(RunClosedError):
            run.submit(urgent)

    async def test_open_run_does_not_finish_when_idle(self):
        @flow
        async def empty(ctx):
            return 12

        runtime = self.runtime()
        run = runtime.start(empty, keep_open=True)
        await asyncio.sleep(0.01)
        self.assertFalse(run.done)
        await run.close_inputs()
        self.assertEqual(await run, 12)

    async def test_subflow_does_not_consume_leaf_slot(self):
        @task
        def value():
            return 42

        @flow
        async def child(ctx):
            return await ctx.submit(value)

        @flow
        async def parent(ctx):
            return await ctx.subflow(child)

        self.assertEqual(await asyncio.wait_for(self.runtime(concurrency=1).arun(parent), 2), 42)

    async def test_all_submitted_children_are_joined(self):
        finished = []

        @task
        async def work():
            await asyncio.sleep(0.01)
            finished.append(True)

        @flow
        async def pipeline(ctx):
            ctx.submit(work)
            return "result"

        self.assertEqual(await self.runtime().arun(pipeline), "result")
        self.assertEqual(finished, [True])

    async def test_retry_wait_releases_slot(self):
        events = []

        @task(priority=10, retry=ExponentialRetry(max_attempts=2, delay=0.03))
        async def flaky():
            count = sum(e.startswith("attempt") for e in events) + 1
            events.append(f"attempt{count}")
            if count == 1:
                raise ValueError("transient")
            return 1

        @task
        async def other():
            events.append("other")

        @flow
        async def pipeline(ctx):
            first, second = ctx.submit(flaky), ctx.submit(other)
            await second
            return await first

        self.assertEqual(await self.runtime(concurrency=1).arun(pipeline), 1)
        self.assertEqual(events, ["attempt1", "other", "attempt2"])

    async def test_failure_skips_data_dependents_and_is_inspectable(self):
        handles = {}

        @task
        def fail():
            raise ValueError("bad input")

        @task
        def downstream(value):
            self.fail("A failed data dependency must not execute")

        @flow
        async def pipeline(ctx):
            handles["source"] = ctx.submit(fail)
            dependent = ctx.submit(downstream, handles["source"])
            return await ctx.all_settled([dependent])

        outcomes = await self.runtime().arun(pipeline)
        self.assertIsInstance(outcomes[0].error, DependencyFailed)
        self.assertIsInstance(handles["source"].details.error, TaskFailed)
        self.assertEqual(len(handles["source"].details.attempts), 1)

    async def test_caught_failure_can_create_new_attempt_node(self):
        @task
        def work(number):
            if number == 0:
                raise ValueError("retry at flow level")
            return number

        @flow
        async def pipeline(ctx):
            try:
                await ctx.submit(work, 0)
            except TaskFailed:
                return await ctx.submit(work, 1)

        self.assertEqual(await self.runtime().arun(pipeline), 1)

    async def test_unobserved_failure_cancels_siblings_after_flow_returns(self):
        stopped = asyncio.Event()

        @task
        async def failure():
            await asyncio.sleep(0.01)
            raise ValueError("boom")

        @task
        async def sibling():
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

        @flow
        async def pipeline(ctx):
            ctx.submit(failure)
            ctx.submit(sibling)

        with self.assertRaises(TaskFailed):
            await asyncio.wait_for(self.runtime(concurrency=2).arun(pipeline), 2)
        self.assertTrue(stopped.is_set())

    async def test_control_policy_cannot_bypass_required_data(self):
        release = asyncio.Event()
        handles = {}

        @task
        async def slow():
            await release.wait()
            return 9

        @task
        async def fast():
            return 1

        @task
        def consumer(value):
            return value

        @flow
        async def pipeline(ctx):
            data, control = ctx.submit(slow), ctx.submit(fast)
            handles["consumer"] = ctx.submit(
                consumer,
                data,
                options=SubmitOptions(
                    after=(control,),
                    dependency_policy=AnySuccessful(),
                ),
            )
            return await handles["consumer"]

        run = self.runtime(concurrency=2).start(pipeline)
        await until(lambda: "consumer" in handles and handles["consumer"].snapshot is not None)
        await asyncio.sleep(0.01)
        self.assertEqual(handles["consumer"].snapshot.state, TaskState.WAITING)
        release.set()
        self.assertEqual(await run, 9)

    async def test_all_settled_control_edge_runs_after_failure(self):
        @task
        async def fail():
            raise ValueError("expected")

        @task
        async def cleanup():
            return "clean"

        @flow
        async def pipeline(ctx):
            source = ctx.submit(fail)
            return await ctx.submit(
                cleanup,
                options=SubmitOptions(
                    after=(source,),
                    dependency_policy=AllSettled(),
                ),
            )

        self.assertEqual(await self.runtime().arun(pipeline), "clean")

    async def test_timeout_waits_for_async_cleanup(self):
        cleaned = []

        @task(timeout=0.01)
        async def slow():
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(True)

        @flow
        async def pipeline(ctx):
            return await ctx.all_settled([ctx.submit(slow)])

        outcomes = await self.runtime(concurrency=1).arun(pipeline)
        self.assertIsInstance(outcomes[0].error.cause, TimeoutError)
        self.assertEqual(cleaned, [True])

    async def test_thread_cancellation_keeps_resource_until_thread_exits(self):
        started, release = threading.Event(), threading.Event()
        ran_second = asyncio.Event()
        handles = {}

        @task(priority=10)
        def thread_work():
            started.set()
            release.wait(2)

        @task
        async def second():
            ran_second.set()

        @flow
        async def pipeline(ctx):
            handles["first"] = ctx.submit(thread_work)
            other = ctx.submit(second)
            return await ctx.all_settled([handles["first"], other])

        run = self.runtime(concurrency=1).start(pipeline)
        try:
            await until(started.is_set)
            await handles["first"].cancel()
            await asyncio.sleep(0.01)
            self.assertFalse(ran_second.is_set())
            self.assertEqual(handles["first"].snapshot.state, TaskState.CANCELLING)
        finally:
            release.set()
        outcomes = await asyncio.wait_for(run.wait(), 2)
        self.assertIsInstance(outcomes[0].error, TaskCancelled)
        self.assertTrue(ran_second.is_set())

    async def test_cancel_before_root_flow_starts(self):
        @flow
        async def pipeline(ctx):
            await asyncio.Event().wait()

        run = self.runtime().start(pipeline)
        await run.cancel()
        with self.assertRaises(TaskCancelled):
            await asyncio.wait_for(run.wait(), 2)

    async def test_aclose_includes_a_queued_start_command(self):
        @flow
        async def pipeline(ctx):
            await asyncio.Event().wait()

        runtime = self.runtime()
        run = runtime.start(pipeline)
        await asyncio.wait_for(runtime.aclose(), 2)
        with self.assertRaises(TaskCancelled):
            await run

    async def test_cancelled_waiter_does_not_cancel_shared_task(self):
        release = asyncio.Event()

        @task
        async def work():
            await release.wait()
            return 7

        @flow
        async def pipeline(ctx):
            handle = ctx.submit(work)
            waiter = asyncio.create_task(handle.result())
            await asyncio.sleep(0)
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            release.set()
            return await handle

        self.assertEqual(await self.runtime().arun(pipeline), 7)

    async def test_cross_run_data_reference_is_rejected(self):
        @task
        async def work(value):
            return value

        @flow
        async def empty(ctx):
            return None

        runtime = self.runtime()
        first = runtime.start(empty, keep_open=True)
        source = first.submit(work, 1)

        @flow
        async def second(ctx):
            return await ctx.submit(work, source)

        with self.assertRaisesRegex(ValueError, "this Runtime and run"):
            await runtime.arun(second)
        await source
        await first.close_inputs()
        await first

    async def test_exception_objects_can_be_successful_values(self):
        @task
        async def work():
            return ValueError("this is data")

        @flow
        async def pipeline(ctx):
            return await ctx.all_settled([ctx.submit(work)])

        outcomes = await self.runtime().arun(pipeline)
        self.assertTrue(outcomes[0].succeeded)
        self.assertIsInstance(outcomes[0].value, ValueError)

    async def test_sys_exit_from_python_task_is_an_execution_failure(self):
        @task
        async def work():
            raise SystemExit("stop this node")

        @flow
        async def pipeline(ctx):
            return await ctx.all_settled([ctx.submit(work)])

        outcomes = await self.runtime().arun(pipeline)
        self.assertIsInstance(outcomes[0].error.cause, RuntimeError)
        self.assertIn("SystemExit", str(outcomes[0].error.cause))

    async def test_admission_rejection_and_backpressured_map(self):
        @task
        async def work(value):
            await asyncio.sleep(0.001)
            return value * 2

        @flow
        async def rejected(ctx):
            return await ctx.all_settled([ctx.submit(work, 1), ctx.submit(work, 2)])

        runtime = self.runtime(admission=BoundedAdmission(1))
        outcomes = await runtime.arun(rejected)
        self.assertTrue(outcomes[0].succeeded)
        self.assertIsInstance(outcomes[1].error, AdmissionError)

        @flow
        async def pipeline(ctx):
            return await ctx.map(work, range(10), max_in_flight=4)

        self.assertEqual(await asyncio.wait_for(runtime.arun(pipeline), 3), list(range(0, 20, 2)))

    async def test_multiple_runs_share_capacity(self):
        active, peak = 0, 0

        @task
        async def work():
            nonlocal active, peak
            active += 1
            peak = max(active, peak)
            await asyncio.sleep(0.01)
            active -= 1

        @flow
        async def pipeline(ctx):
            await ctx.submit(work)

        runtime = self.runtime(concurrency=1)
        await asyncio.gather(runtime.arun(pipeline), runtime.arun(pipeline))
        self.assertEqual(peak, 1)

    async def test_policy_timer_wakes_without_a_completion_event(self):
        class DelayedScheduler(PartialOrderScheduler):
            def schedule(self, snapshot):
                if snapshot.ready and snapshot.now < 5:
                    return ScheduleDecision(snapshot.revision, wake_at=5)
                return super().schedule(snapshot)

        @task
        async def work():
            return 42

        @flow
        async def pipeline(ctx):
            return await ctx.submit(work)

        clock = VirtualClock()
        runtime = self.runtime(clock=clock, scheduler=DelayedScheduler())
        run = runtime.start(pipeline)
        await until(lambda: any(t.state == TaskState.READY for t in run.tasks))
        clock.advance(5)
        self.assertEqual(await asyncio.wait_for(run.wait(), 2), 42)

    async def test_invalid_scheduler_decision_fails_instead_of_hanging(self):
        class DuplicateScheduler(Scheduler):
            def schedule(self, snapshot):
                starts = ()
                if snapshot.ready:
                    starts = (Start(snapshot.ready[0].id),) * 2
                return ScheduleDecision(snapshot.revision, starts)

        @task
        async def work():
            self.fail("An invalid decision must not start a worker")

        @flow
        async def pipeline(ctx):
            return await ctx.submit(work)

        runtime = self.runtime(scheduler=DuplicateScheduler())
        with self.assertRaises(SchedulingError):
            await asyncio.wait_for(runtime.arun(pipeline), 2)
        self.assertTrue(all(t.state.terminal for t in runtime.state.snapshot().tasks.values()))

    async def test_progress_reports_update_snapshots(self):
        @task(context=True)
        async def work(ctx):
            ctx.report(percent=50)
            await asyncio.sleep(0.01)
            return 1

        @flow
        async def pipeline(ctx):
            return await ctx.submit(work)

        runtime = self.runtime()
        self.assertEqual(await runtime.arun(pipeline), 1)
        view = next(iter(runtime.state.snapshot().tasks.values()))
        self.assertEqual(view.metrics["percent"], 50)

    async def test_leaf_cannot_submit_children_using_captured_flow_context(self):
        @flow
        async def pipeline(ctx):
            @task
            async def child():
                ctx.submit(child)

            return await ctx.all_settled([ctx.submit(child)])

        outcomes = await self.runtime(concurrency=1).arun(pipeline)
        self.assertIsInstance(outcomes[0].error.cause, RuntimeError)


class SyncRuntimeTests(unittest.TestCase):
    def test_same_runtime_can_run_sequential_event_loops(self):
        @task
        def double(value):
            return value * 2

        @flow
        async def pipeline(ctx, value):
            return await ctx.submit(double, value)

        runtime = EventLoopRuntime()
        self.assertEqual(runtime.run(pipeline, 1), 2)
        self.assertEqual(runtime.run(pipeline, 2), 4)
