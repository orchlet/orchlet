from .contracts import SessionPolicy


class FreshSession(SessionPolicy):
    def bind(self, task_id, attempt):
        return None

    def resource_key(self):
        return None


class FixedSession(SessionPolicy):
    """Continue an existing backend session, with a runtime-wide exclusive reservation."""

    def __init__(self, session_id):
        if not isinstance(session_id, str) or not session_id:
            raise ValueError("session_id must be a nonempty string")
        self.session_id = session_id

    def bind(self, task_id, attempt):
        return self.session_id

    def resource_key(self):
        return f"session:{self.session_id}"
