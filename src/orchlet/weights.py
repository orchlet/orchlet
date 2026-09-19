from __future__ import annotations

from collections.abc import Callable, Iterable

from .contracts import WeightModel
from .models import ScheduleSnapshot, TaskView


class ConstantWeight(WeightModel):
    def __init__(self, value: float = 0.0) -> None:
        self.value: float = float(value)

    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        return self.value


class AgingWeight(WeightModel):
    def __init__(self, rate: float = 1.0) -> None:
        self.rate = rate

    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        since = task.ready_at if task.ready_at is not None else snapshot.now
        return self.rate * max(0.0, snapshot.now - since)


class MetricWeight(WeightModel):
    def __init__(self, key: str, coefficient: float = 1.0, default: float = 0.0) -> None:
        self.key: str = key
        self.coefficient: float = coefficient
        self.default: float = default

    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        return self.coefficient * float(task.metrics.get(self.key, self.default))


class KnownDownstreamWeight(WeightModel):
    """Count known direct waiting consumers, not hypothetical future dynamic tasks."""

    def __init__(self, coefficient: float = 1.0) -> None:
        self.coefficient = coefficient

    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        return self.coefficient * task.waiting_dependents


class CompositeWeightModel(WeightModel):
    def __init__(self, models: Iterable[WeightModel]) -> None:
        self.models: tuple[WeightModel, ...] = tuple(models)

    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        return sum(model.evaluate(task, snapshot) for model in self.models)


class FunctionWeight(WeightModel):
    def __init__(self, function: Callable[[TaskView, ScheduleSnapshot], float]) -> None:
        self.function = function

    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        return float(self.function(task, snapshot))
