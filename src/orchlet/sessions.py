from __future__ import annotations

from typing import cast

from .contracts import SessionPolicy
from .models import AttemptContext


class FreshSession(SessionPolicy):
    def bind(self, task_id: str, attempt: AttemptContext) -> str | None:
        return None

    def resource_key(self) -> str | None:
        return None


class FixedSession(SessionPolicy):
    """Continue an existing backend session, with a runtime-wide exclusive reservation."""

    def __init__(self, session_id: str) -> None:
        if not isinstance(cast(object, session_id), str) or not session_id:
            raise ValueError("session_id must be a nonempty string")
        self.session_id = session_id

    def bind(self, task_id: str, attempt: AttemptContext) -> str | None:
        return self.session_id

    def resource_key(self) -> str | None:
        return f"session:{self.session_id}"
