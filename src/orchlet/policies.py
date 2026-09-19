from math import isfinite

from .contracts import AdmissionPolicy, DependencyPolicy, FailurePolicy, RetryPolicy
from .models import Gate, RetryDecision, TaskState


class AllSuccessful(DependencyPolicy):
    def evaluate(self, states):
        if any(s.terminal and s != TaskState.SUCCEEDED for s in states):
            return Gate.IMPOSSIBLE
        return Gate.PASS if all(s == TaskState.SUCCEEDED for s in states) else Gate.WAIT


class AllSettled(DependencyPolicy):
    def evaluate(self, states):
        return Gate.PASS if all(s.terminal for s in states) else Gate.WAIT


class KSuccessful(DependencyPolicy):
    def __init__(self, count):
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError("count must be a nonnegative integer")
        self.count = count

    def evaluate(self, states):
        successes = sum(s == TaskState.SUCCEEDED for s in states)
        if successes >= self.count:
            return Gate.PASS
        if successes + sum(not s.terminal for s in states) < self.count:
            return Gate.IMPOSSIBLE
        return Gate.WAIT


class AnySuccessful(KSuccessful):
    def __init__(self):
        super().__init__(1)


class BoundedAdmission(AdmissionPolicy):
    def __init__(self, max_pending=10000):
        if isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending < 1:
            raise ValueError("max_pending must be a positive integer")
        self.max_pending = max_pending

    def admit(self, task, outstanding):
        return outstanding < self.max_pending


class NoRetry(RetryPolicy):
    def decide(self, failure, history):
        return RetryDecision(False)


class ExponentialRetry(RetryPolicy):
    def __init__(self, max_attempts=3, delay=1.0, max_delay=60.0, retry_on=(Exception,)):
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise ValueError("max_attempts includes the initial attempt and must be positive")
        if any(not isfinite(v) or v < 0 for v in (delay, max_delay)):
            raise ValueError("Retry delays must be finite and nonnegative")
        self.max_attempts, self.delay, self.max_delay = max_attempts, delay, max_delay
        self.retry_on = retry_on

    def decide(self, failure, history):
        if len(history) >= self.max_attempts or not isinstance(failure, self.retry_on):
            return RetryDecision(False)
        delay = min(self.max_delay, self.delay * 2 ** min(len(history) - 1, 60))
        return RetryDecision(True, delay)


class FailScope(FailurePolicy):
    def fail_scope(self, unobserved_errors):
        return bool(unobserved_errors)


class CollectFailures(FailurePolicy):
    def fail_scope(self, unobserved_errors):
        return False
