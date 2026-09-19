import asyncio
import logging
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.logging import RichHandler

from orchlet import EventLoopRuntime, FlowContext, configure_logging, flow, get_logger, task
from orchlet.events import JsonlJournal
from orchlet.logging import log_event
from orchlet.models import RuntimeEvent, ScheduleDecision, ScheduleSnapshot
from orchlet.policies import ExponentialRetry
from orchlet.schedulers import PartialOrderScheduler


def event_of(record: logging.LogRecord) -> RuntimeEvent:
    event = getattr(record, "orchlet_event", None)
    assert isinstance(event, RuntimeEvent)
    return event


class RecordHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class BrokenHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        raise RuntimeError("Log destination is unavailable")


class LoggingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.package = get_logger()
        self.runtime_logger = get_logger("runtime")
        self.saved: list[tuple[logging.Logger, list[logging.Handler], int, bool, bool]] = []
        for logger in (logging.getLogger(), self.package, self.runtime_logger):
            self.saved.append(
                (logger, list(logger.handlers), logger.level, logger.propagate, logger.disabled)
            )
        self.package.handlers = [logging.NullHandler()]
        self.package.setLevel(logging.NOTSET)
        self.package.propagate = False
        self.package.disabled = False
        self.runtime_logger.handlers = []
        self.runtime_logger.setLevel(logging.NOTSET)
        self.runtime_logger.propagate = True
        self.runtime_logger.disabled = False
        self.runtimes: list[EventLoopRuntime] = []

    async def asyncTearDown(self):
        for runtime in self.runtimes:
            await asyncio.wait_for(runtime.aclose(), 3)

    def tearDown(self):
        for logger, handlers, level, propagate, disabled in self.saved:
            for handler in logger.handlers:
                if handler not in handlers:
                    handler.close()
            logger.handlers = handlers
            logger.setLevel(level)
            logger.propagate = propagate
            logger.disabled = disabled

    def runtime(self, **kwargs: Any) -> EventLoopRuntime:
        runtime = EventLoopRuntime(**kwargs)
        self.runtimes.append(runtime)
        return runtime

    def test_rich_handler_preserves_literal_text_and_filters_levels(self):
        output = StringIO()
        handler = configure_logging(
            console=Console(file=output, width=160, force_terminal=False), show_time=False
        )
        self.assertIsInstance(handler, RichHandler)
        logger = get_logger("application")
        logger.debug("hidden debug details")
        message = '[bold]literal[/bold] [unknown-tag] {"items": [1, 2]}'
        logger.info("Model response: %s", message)
        self.assertIn(message, output.getvalue())
        self.assertIn("INFO", output.getvalue())
        self.assertNotIn("hidden debug details", output.getvalue())
        self.assertNotIn("\x1b[", output.getvalue())

    def test_default_console_writes_to_stderr(self):
        stdout, stderr = StringIO(), StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            configure_logging(show_time=False)
            get_logger().warning("Console destination check")
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("Console destination check", stderr.getvalue())
        self.assertIn("WARNING", stderr.getvalue())

    def test_reconfiguration_preserves_application_handlers_without_duplicates(self):
        root = logging.getLogger()
        root_handler, app_handler = RecordHandler(), RecordHandler()
        root.addHandler(root_handler)
        self.package.addHandler(app_handler)
        root_handlers, root_level = list(root.handlers), root.level
        first, second = StringIO(), StringIO()
        configure_logging(console=Console(file=first), show_time=False)
        get_logger().info("first message")
        configure_logging(console=Console(file=second), show_time=False)
        get_logger().info("second message")
        self.assertEqual(root.handlers, root_handlers)
        self.assertEqual(root.level, root_level)
        self.assertIn(app_handler, self.package.handlers)
        self.assertEqual(len(app_handler.records), 2)
        self.assertEqual(root_handler.records, [])
        self.assertEqual(first.getvalue().count("first message"), 1)
        self.assertNotIn("second message", first.getvalue())
        self.assertEqual(second.getvalue().count("second message"), 1)

    def test_rich_exception_rendering(self):
        output = StringIO()
        configure_logging(
            console=Console(file=output, width=160, force_terminal=False), show_time=False
        )
        try:
            raise ValueError("Example exception")
        except ValueError:
            get_logger().exception("Operation failed")
        self.assertIn("Operation failed", output.getvalue())
        self.assertIn("ValueError: Example exception", output.getvalue())
        self.assertIn("Traceback", output.getvalue())

    async def test_runtime_logs_keep_structured_journal_events_and_retry_levels(self):
        output, capture = StringIO(), RecordHandler()
        configure_logging("DEBUG", console=Console(file=output, width=160), show_time=False)
        self.package.addHandler(capture)
        attempts = 0

        @task(retry=ExponentialRetry(max_attempts=2, delay=0))
        async def flaky():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ValueError("Retry this operation")
            return 42

        @task
        async def fail():
            raise ValueError("Expected task failure")

        @flow
        async def pipeline(ctx: FlowContext):
            return await ctx.all_settled([ctx.submit(flaky), ctx.submit(fail)])

        with tempfile.TemporaryDirectory() as directory:
            journal = JsonlJournal(Path(directory) / "events.jsonl")
            outcomes = await self.runtime(journal=journal).arun(pipeline)
            self.assertEqual(outcomes[0].value, 42)
            self.assertFalse(outcomes[1].succeeded)
            self.assertEqual(tuple(event_of(r) for r in capture.records), journal.read())
        records = capture.records
        retries = [r for r in records if event_of(r).kind == "task_retry"]
        self.assertEqual([r.levelno for r in retries], [logging.WARNING])
        failures = [r for r in records if event_of(r).data.get("state") == "failed"]
        self.assertEqual([r.levelno for r in failures], [logging.ERROR])
        self.assertTrue(any(r.levelno == logging.DEBUG for r in records))
        for record in records:
            event = event_of(record)
            if event.task_id is not None:
                self.assertIn(event.task_id, record.getMessage())
        self.assertIn("Retry this operation", output.getvalue())

    def test_timeout_skip_cancellation_and_run_failure_severities(self):
        capture = RecordHandler()
        self.package.setLevel(logging.DEBUG)
        self.package.addHandler(capture)
        cases: list[tuple[str, dict[str, str], int]] = [
            ("task_timeout", {}, logging.WARNING),
            ("task_finished", {"state": "skipped"}, logging.WARNING),
            ("task_finished", {"state": "cancelled"}, logging.INFO),
            ("run_finished", {"error": "Flow failed"}, logging.ERROR),
            ("scheduler_waiting", {"reason": "Policy is waiting"}, logging.WARNING),
        ]
        for number, (kind, data, expected) in enumerate(cases):
            log_event(RuntimeEvent(number, 0, kind, run_id="run", data=data))
            self.assertEqual(capture.records[-1].levelno, expected)

    async def test_broken_logging_does_not_break_execution_or_abort_cleanup(self):
        self.package.setLevel(logging.DEBUG)
        self.package.addHandler(BrokenHandler())

        @task
        async def value():
            return 42

        @flow
        async def pipeline(ctx: FlowContext):
            return await ctx.submit(value)

        self.assertEqual(await self.runtime().arun(pipeline), 42)

        class BrokenScheduler(PartialOrderScheduler):
            def schedule(self, snapshot: ScheduleSnapshot) -> ScheduleDecision:
                raise RuntimeError("Scheduling failed")

        with self.assertRaisesRegex(RuntimeError, "Scheduling failed"):
            await asyncio.wait_for(self.runtime(scheduler=BrokenScheduler()).arun(pipeline), 2)

    def test_library_is_quiet_without_console_configuration(self):
        script = """
from orchlet import EventLoopRuntime, flow, task

@task
def fail():
    raise ValueError("Expected failure")

@flow
async def pipeline(ctx):
    return await ctx.all_settled([ctx.submit(fail)])

EventLoopRuntime().run(pipeline)
"""
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=True, timeout=5
        )
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")
