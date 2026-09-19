from __future__ import annotations

from collections.abc import Sequence
from math import isfinite
from typing import cast

from .contracts import AdmissionPolicy, DependencyPolicy, FailurePolicy, RetryPolicy
from .models import Attempt, Gate, RetryDecision, TaskState, TaskView


class AllSuccessful(DependencyPolicy):
    def evaluate(self, states: Sequence[TaskState]) -> Gate:
        if any(s.terminal and s != TaskState.SUCCEEDED for s in states):
            return Gate.IMPOSSIBLE
        return Gate.PASS if all(s == TaskState.SUCCEEDED for s in states) else Gate.WAIT


class AllSettled(DependencyPolicy):
    def evaluate(self, states: Sequence[TaskState]) -> Gate:
        return Gate.PASS if all(s.terminal for s in states) else Gate.WAIT


class KSuccessful(DependencyPolicy):
    def __init__(self, count: int) -> None:
        if isinstance(count, bool) or not isinstance(cast(object, count), int) or count < 0:
            raise ValueError("count must be a nonnegative integer")
        self.count = count

    def evaluate(self, states: Sequence[TaskState]) -> Gate:
        successes = sum(s == TaskState.SUCCEEDED for s in states)
        if successes >= self.count:
            return Gate.PASS
        if successes + sum(not s.terminal for s in states) < self.count:
            return Gate.IMPOSSIBLE
        return Gate.WAIT


class AnySuccessful(KSuccessful):
    def __init__(self) -> None:
        super().__init__(1)


class BoundedAdmission(AdmissionPolicy):
    def __init__(self, max_pending: int = 10000) -> None:
        if (
            isinstance(max_pending, bool)
            or not isinstance(cast(object, max_pending), int)
            or max_pending < 1
        ):
            raise ValueError("max_pending must be a positive integer")
        self.max_pending = max_pending

    def admit(self, task: TaskView, outstanding: int) -> bool:
        return outstanding < self.max_pending


class NoRetry(RetryPolicy):
    def decide(self, failure: BaseException, history: Sequence[Attempt]) -> RetryDecision:
        return RetryDecision(False)


class ExponentialRetry(RetryPolicy):
    def __init__(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        max_delay: float = 60.0,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
    ) -> None:
        if (
            isinstance(max_attempts, bool)
            or not isinstance(cast(object, max_attempts), int)
            or max_attempts < 1
        ):
            raise ValueError("max_attempts includes the initial attempt and must be positive")
        if any(not isfinite(v) or v < 0 for v in (delay, max_delay)):
            raise ValueError("Retry delays must be finite and nonnegative")
        self.max_attempts: int = max_attempts
        self.delay: float = delay
        self.max_delay: float = max_delay
        self.retry_on = retry_on

    def decide(self, failure: BaseException, history: Sequence[Attempt]) -> RetryDecision:
        if len(history) >= self.max_attempts or not isinstance(failure, self.retry_on):
            return RetryDecision(False)
        delay = min(self.max_delay, self.delay * 2 ** min(len(history) - 1, 60))
        return RetryDecision(True, delay)


class FailScope(FailurePolicy):
    def fail_scope(self, unobserved_errors: Sequence[BaseException]) -> bool:
        return bool(unobserved_errors)


class CollectFailures(FailurePolicy):
    def fail_scope(self, unobserved_errors: Sequence[BaseException]) -> bool:
        return False
