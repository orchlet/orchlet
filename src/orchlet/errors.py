"""Public errors. Execution failures retain the original exception as their cause."""


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


class TaskFailed(OrchletError):
    def __init__(self, task_id: str, cause: BaseException) -> None:
        self.task_id = task_id
        self.cause = cause
        super().__init__(f"{task_id}: {type(cause).__name__}: {cause}")
        self.__cause__: BaseException | None = cause


class TaskCancelled(OrchletError):
    pass


class DependencyFailed(OrchletError):
    pass


class OutputValidationError(OrchletError):
    def __init__(self, message: str, raw_text: str = "") -> None:
        self.raw_text = raw_text
        super().__init__(message)


class BackendError(OrchletError):
    def __init__(
        self, message: str, *, stdout: str = "", stderr: str = "", exit_code: int | None = None
    ) -> None:
        self.stdout: str = stdout
        self.stderr: str = stderr
        self.exit_code: int | None = exit_code
        super().__init__(message)
