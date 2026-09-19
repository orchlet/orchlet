"""Standard Python logging with opt-in Rich console rendering."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

from .events import json_value
from .models import RuntimeEvent, TaskState

_package_logger = logging.getLogger("orchlet")
_package_logger.addHandler(logging.NullHandler())


class _ConsoleHandler(RichHandler):
    """Identify handlers owned by configure_logging without replacing application handlers."""


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a logger in the orchlet namespace."""
    if not name or name == "orchlet":
        return _package_logger
    return logging.getLogger(name if name.startswith("orchlet.") else f"orchlet.{name}")


def configure_logging(
    level: int | str = logging.INFO,
    *,
    console: Console | None = None,
    show_time: bool = True,
    show_path: bool = False,
    rich_tracebacks: bool = True,
) -> RichHandler:
    """Configure Orchlet's console handler, preserving the root and application handlers.

    Repeated calls replace the previous Orchlet console handler. By default, logs
    go to stderr with terminal-aware colors and literal message text. Applications
    can instead configure standard logging themselves without calling this helper.
    """
    handler = _ConsoleHandler(
        level=level,
        console=console if console is not None else Console(stderr=True),
        show_time=show_time,
        show_path=show_path,
        log_time_format="%H:%M:%S",
        markup=False,
        rich_tracebacks=rich_tracebacks,
        tracebacks_show_locals=False,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    for previous in tuple(_package_logger.handlers):
        if isinstance(previous, _ConsoleHandler):
            _package_logger.removeHandler(previous)
            previous.close()
    _package_logger.addHandler(handler)
    _package_logger.setLevel(level)
    _package_logger.disabled = False
    _package_logger.propagate = False
    return handler


_runtime_logger = get_logger("runtime")


def log_event(event: RuntimeEvent) -> None:
    """Log a committed runtime event and retain the original event on its LogRecord."""
    level = logging.DEBUG
    if event.kind in {"task_retry", "task_timeout", "scheduler_waiting"}:
        level = logging.WARNING
    elif event.kind == "task_finished":
        state = event.data.get("state")
        if state == TaskState.FAILED:
            level = logging.ERROR
        elif state == TaskState.SKIPPED:
            level = logging.WARNING
        else:
            level = logging.INFO
    elif event.kind == "run_finished":
        level = logging.ERROR if event.data.get("error") is not None else logging.INFO
    elif event.kind in {"run_started", "task_started"}:
        level = logging.INFO
    if not _runtime_logger.isEnabledFor(level):
        return
    try:
        details = " ".join(
            f"{key}={json_value(value)!r}" for key, value in event.data.items() if value is not None
        )
        _runtime_logger.log(
            level,
            "%s [%s]%s",
            event.kind.replace("_", " "),
            event.task_id or event.run_id or "runtime",
            f": {details}" if details else "",
            extra={"orchlet_event": event},
        )
    except Exception:
        # Diagnostic formatting or application handlers must not interrupt orchestration.
        pass


def log_runtime_error(error: BaseException) -> None:
    """Report an engine failure without allowing a logging error to interrupt cleanup."""
    try:
        _runtime_logger.error(
            "Runtime aborted: %s",
            error,
            exc_info=(type(error), error, error.__traceback__),
        )
    except Exception:
        pass
