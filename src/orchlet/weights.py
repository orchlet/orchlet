from .contracts import WeightModel


class ConstantWeight(WeightModel):
    def __init__(self, value=0.0):
        self.value = float(value)

    def evaluate(self, task, snapshot):
        return self.value


class AgingWeight(WeightModel):
    def __init__(self, rate=1.0):
        self.rate = rate

    def evaluate(self, task, snapshot):
        since = task.ready_at if task.ready_at is not None else snapshot.now
        return self.rate * max(0.0, snapshot.now - since)


class MetricWeight(WeightModel):
    def __init__(self, key, coefficient=1.0, default=0.0):
        self.key, self.coefficient, self.default = key, coefficient, default

    def evaluate(self, task, snapshot):
        return self.coefficient * float(task.metrics.get(self.key, self.default))


class KnownDownstreamWeight(WeightModel):
    """Count known direct waiting consumers, not hypothetical future dynamic tasks."""

    def __init__(self, coefficient=1.0):
        self.coefficient = coefficient

    def evaluate(self, task, snapshot):
        return self.coefficient * task.waiting_dependents


class CompositeWeightModel(WeightModel):
    def __init__(self, models):
        self.models = tuple(models)

    def evaluate(self, task, snapshot):
        return sum(model.evaluate(task, snapshot) for model in self.models)


class FunctionWeight(WeightModel):
    def __init__(self, function):
        self.function = function

    def evaluate(self, task, snapshot):
        return float(self.function(task, snapshot))
