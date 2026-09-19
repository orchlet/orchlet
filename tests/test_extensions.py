import asyncio
import inspect
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import cast

from orchlet import EventLoopRuntime, FlowContext, contracts, flow, task
from orchlet.clocks import VirtualClock
from orchlet.events import AsyncioEventTransport, JsonlJournal, MemoryEventJournal
from orchlet.models import RuntimeEvent
from orchlet.runners import SimulatedRunner


class ExtensionTests(unittest.IsolatedAsyncioTestCase):
    async def test_journal_contract_for_memory_and_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            for journal in (MemoryEventJournal(), JsonlJournal(Path(directory) / "events.jsonl")):
                with self.subTest(implementation=type(journal).__name__):
                    event = RuntimeEvent(1, 2.0, "test", data={"nested": {"value": [1, 2]}})
                    await journal.append(event)
                    self.assertEqual(journal.read(), (event,))

    async def test_transport_preserves_fifo_and_bounded_drain(self):
        transport = AsyncioEventTransport[int]()
        transport.open()
        for number in range(5):
            transport.send(number)
        self.assertEqual(await transport.receive(), 0)
        self.assertEqual(transport.drain(2), [1, 2])
        self.assertEqual(transport.drain(100), [3, 4])
        self.assertTrue(transport.empty())

    async def test_virtual_clock_and_simulated_runner(self):
        clock = VirtualClock()

        @task(kind="simulation")
        def work(value: int) -> int:
            raise AssertionError("The simulated runner supplies the result")

        @flow
        async def pipeline(ctx: FlowContext):
            return await ctx.submit(work, 21)

        runtime = EventLoopRuntime(
            clock=clock,
            runners={
                "simulation": SimulatedRunner(lambda request: request.args[0] * 2, duration=10),
            },
        )
        run = runtime.start(pipeline)
        async with asyncio.timeout(2):
            while clock.next_deadline is None:
                await asyncio.sleep(0)
        self.assertEqual(clock.next_deadline, 10)
        self.assertFalse(run.done)
        clock.advance_to_next()
        self.assertEqual(await asyncio.wait_for(run.wait(), 2), 42)
        self.assertEqual(clock.now(), 10)

    async def test_virtual_timer_cancellation(self):
        clock = VirtualClock()
        events: list[str] = []
        cancelled = clock.schedule_at(1, lambda: events.append("cancelled"))
        clock.schedule_at(2, lambda: events.append("fired"))
        cancelled.cancel()
        clock.advance(3)
        self.assertEqual(events, ["fired"])
        self.assertIsNone(clock.next_deadline)

    async def test_run_writes_reusable_diagnostic_journal(self):
        @task
        def value():
            return 7

        @flow
        async def pipeline(ctx: FlowContext):
            return await ctx.submit(value)

        with tempfile.TemporaryDirectory() as directory:
            journal = JsonlJournal(Path(directory) / "events.jsonl")
            result = await EventLoopRuntime(journal=journal).arun(pipeline)
            self.assertEqual(result, 7)
            events = journal.read()
            self.assertEqual(events[0].kind, "run_started")
            self.assertEqual(events[-1].kind, "run_finished")
            self.assertEqual([e.sequence for e in events], list(range(1, len(events) + 1)))
            self.assertIn("task_started", [e.kind for e in events])


class ContractTests(unittest.TestCase):
    def test_extension_points_are_real_abstract_classes(self):
        abstractions = [
            value
            for _, value in inspect.getmembers(contracts, inspect.isclass)
            if value.__module__ == contracts.__name__ and inspect.isabstract(value)
        ]
        self.assertGreaterEqual(len(abstractions), 20)
        for abstraction in abstractions:
            with self.subTest(name=abstraction.__name__), self.assertRaises(TypeError):
                # This test deliberately bypasses construction typing to test ABC enforcement.
                cast(Callable[[], object], abstraction)()
