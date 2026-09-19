import asyncio
import json
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path

from .contracts import EventJournal, EventTransport
from .models import RuntimeEvent


class AsyncioEventTransport(EventTransport):
    def __init__(self):
        self._queue = None

    def open(self):
        if self._queue is not None and not self._queue.empty():
            raise RuntimeError("Cannot reset a nonempty event transport")
        self._queue = asyncio.Queue()

    def send(self, message):
        if self._queue is None:
            raise RuntimeError("Transport has not been opened")
        self._queue.put_nowait(message)

    async def receive(self):
        return await self._queue.get()

    def drain(self, limit):
        messages = []
        for _ in range(limit):
            try:
                messages.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return messages

    def empty(self):
        return self._queue is None or self._queue.empty()


class MemoryEventJournal(EventJournal):
    def __init__(self):
        self._events = []

    async def append(self, event):
        self._events.append(event)

    def read(self):
        return tuple(self._events)


def json_value(value):
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {f.name: json_value(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [json_value(v) for v in value]
    if value is None or isinstance(value, (str, bool, float, int)):
        return value
    return repr(value)


class JsonlJournal(EventJournal):
    """Diagnostic journal. It does not claim atomic persistence with a StateStore."""

    def __init__(self, path):
        self.path = Path(path)

    async def append(self, event):
        line = json.dumps(json_value(event), ensure_ascii=False, allow_nan=False) + "\n"

        def write():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(line)

        await asyncio.to_thread(write)

    def read(self):
        if not self.path.exists():
            return ()
        return tuple(
            RuntimeEvent(**json.loads(line)) for line in self.path.read_text().splitlines()
        )
