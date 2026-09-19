"""Typed state and callbacks shared by the runtime and its public handles."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from .contracts import StateStore
from .models import TaskResult

if TYPE_CHECKING:
    from .definitions import FlowDef, TaskDef
    from .handles import FlowHandle, TaskHandle
    from .runtime import SubmitOptions


@dataclass
class Completion[T]:
    future: asyncio.Future[T]
    observed: bool = False


class Command(Protocol):
    def __call__(
        self,
        kind: str,
        run_id: str | None = None,
        task_id: str | None = None,
        payload: Any = None,
    ) -> Awaitable[None]: ...


class Submit(Protocol):
    def __call__[T](
        self,
        run_id: str,
        scope_id: str | None,
        definition: TaskDef[..., T],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        options: SubmitOptions | None = None,
        deferred: bool = False,
    ) -> TaskHandle[T]: ...


class Subflow(Protocol):
    def __call__[T](
        self,
        run_id: str,
        parent: str,
        definition: FlowDef[..., T] | Callable[..., Awaitable[T]],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> FlowHandle[T]: ...


@dataclass(frozen=True)
class RuntimeBridge:
    state: StateStore
    command: Command
    submit: Submit
    subflow: Subflow
    task: Callable[[str], TaskHandle[Any]]
    details: Callable[[str], TaskResult[Any] | None]
    completion: Callable[[str], Completion[Any]]
    admission: Callable[[str], asyncio.Future[None]]
