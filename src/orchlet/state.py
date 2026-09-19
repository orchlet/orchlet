from .contracts import StateStore
from .errors import RevisionConflict
from .models import StateSnapshot


class MemoryStateStore(StateStore):
    def __init__(self):
        self._snapshot = StateSnapshot(0, {})

    def snapshot(self):
        return self._snapshot

    async def commit(self, expected_revision, changes):
        if expected_revision != self._snapshot.revision:
            raise RevisionConflict(f"Expected {expected_revision}, found {self._snapshot.revision}")
        self._snapshot = StateSnapshot(expected_revision + 1, {**self._snapshot.tasks, **changes})
        return self._snapshot
