"""Public errors. Execution failures retain the original exception as their cause."""

from collections.abc import Sequence

from .models import BatchFailure


class OrchletError(Exception):
    pass


class ConfigurationError(OrchletError):
    pass


class AdmissionError(OrchletError):
    pass


class RunClosedError(OrchletError):
    pass


class SchedulingError(OrchletError):
    pass


class RevisionConflict(OrchletError):
    pass


class RecoveryError(OrchletError):
    """A saved run cannot be safely continued with the current flow or inputs."""


class TaskFailed(OrchletError):
    def __init__(self, task_id: str, cause: BaseException) -> None:
        self.task_id = task_id
        self.cause = cause
        super().__init__(f"{task_id}: {type(cause).__name__}: {cause}")
        self.__cause__: BaseException | None = cause


class TaskCancelled(OrchletError):
    pass


class BatchFailed(OrchletError):
    """Terminal member failures, including their logical keys and original causes."""

    def __init__(self, batch_id: str, failures: Sequence[BatchFailure]) -> None:
        self.batch_id: str = batch_id
        self.failures: tuple[BatchFailure, ...] = tuple(failures)
        if not self.failures:
            raise ValueError("BatchFailed requires at least one member failure")
        keys = ", ".join(failure.key for failure in self.failures)
        super().__init__(f"{len(self.failures)} batch members failed ({keys})")
        self.__cause__: BaseException | None = ExceptionGroup(
            f"Failures in {batch_id}", [failure.error for failure in self.failures]
        )


class DependencyFailed(OrchletError):
    pass


class OutputValidationError(OrchletError):
    def __init__(
        self,
        message: str,
        raw_text: str = "",
        *,
        stdout: str = "",
        stderr: str = "",
        session_id: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        self.raw_text = raw_text
        self.stdout = stdout
        self.stderr = stderr
        self.session_id = session_id
        self.exit_code = exit_code
        super().__init__(message)


class BackendError(OrchletError):
    def __init__(
        self,
        message: str,
        *,
        stdout: str = "",
        stderr: str = "",
        exit_code: int | None = None,
        raw_text: str | None = None,
        session_id: str | None = None,
    ) -> None:
        self.stdout: str = stdout
        self.stderr: str = stderr
        self.exit_code: int | None = exit_code
        self.raw_text = raw_text
        self.session_id = session_id
        super().__init__(message)
