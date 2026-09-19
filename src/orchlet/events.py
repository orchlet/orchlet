from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from os import PathLike
from pathlib import Path
from typing import Any, cast

from .contracts import EventJournal, EventTransport
from .models import RuntimeEvent


class AsyncioEventTransport[M](EventTransport[M]):
    def __init__(self) -> None:
        self._queue: asyncio.Queue[M] | None = None

    def open(self) -> None:
        if self._queue is not None and not self._queue.empty():
            raise RuntimeError("Cannot reset a nonempty event transport")
        self._queue = asyncio.Queue()

    def send(self, message: M) -> None:
        if self._queue is None:
            raise RuntimeError("Transport has not been opened")
        self._queue.put_nowait(message)

    async def receive(self) -> M:
        if self._queue is None:
            raise RuntimeError("Transport has not been opened")
        return await self._queue.get()

    def drain(self, limit: int) -> list[M]:
        if self._queue is None:
            raise RuntimeError("Transport has not been opened")
        messages: list[M] = []
        for _ in range(limit):
            try:
                messages.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return messages

    def empty(self) -> bool:
        return self._queue is None or self._queue.empty()


class MemoryEventJournal(EventJournal):
    def __init__(self) -> None:
        self._events: list[RuntimeEvent] = []

    async def append(self, event: RuntimeEvent) -> None:
        self._events.append(event)

    def read(self) -> tuple[RuntimeEvent, ...]:
        return tuple(self._events)


def json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: json_value(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        return {str(k): json_value(v) for k, v in cast(Mapping[Any, Any], value).items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [
            json_value(v)
            for v in cast(list[Any] | tuple[Any, ...] | set[Any] | frozenset[Any], value)
        ]
    if isinstance(value, PathLike):
        return str(cast(PathLike[str], value))
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if value is None or isinstance(value, (str, bool, float, int)):
        return value
    return repr(value)


class JsonlJournal(EventJournal):
    """Diagnostic journal. It does not claim atomic persistence with a StateStore."""

    def __init__(self, path: str | PathLike[str]) -> None:
        self.path: Path = Path(path)

    async def append(self, event: RuntimeEvent) -> None:
        line = json.dumps(json_value(event), ensure_ascii=False, allow_nan=False) + "\n"

        def write() -> None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(line)

        await asyncio.to_thread(write)

    def read(self) -> tuple[RuntimeEvent, ...]:
        if not self.path.exists():
            return ()
        return tuple(
            RuntimeEvent(**json.loads(line)) for line in self.path.read_text().splitlines()
        )
