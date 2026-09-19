"""Batch schedulers recompute live weights at every decision, with stable FIFO ties."""

from math import isfinite

from .contracts import ReadyIndex, Scheduler
from .errors import SchedulingError
from .models import Ordering, ScheduleDecision, Start
from .priorities import NumericPriorityOrder
from .resources import TokenResourceAllocator
from .weights import ConstantWeight


class ScanReadyIndex(ReadyIndex):
    def __init__(self):
        self._tasks = ()

    def update(self, tasks):
        self._tasks = tuple(tasks)

    def candidates(self):
        return self._tasks


class _GreedyScheduler(Scheduler):
    def __init__(self, *, weights=None, index=None, refresh_interval=None):
        if refresh_interval is not None and (
            not isfinite(refresh_interval) or refresh_interval <= 0
        ):
            raise ValueError("refresh_interval must be positive and finite")
        self.weights = weights if weights is not None else ConstantWeight()
        self.index = index if index is not None else ScanReadyIndex()
        self.resources = TokenResourceAllocator()
        self.refresh_interval = refresh_interval

    def bind_resources(self, resources):
        self.resources = resources

    def _frontier(self, feasible):
        return feasible

    def _score(self, task, snapshot):
        return self.weights.evaluate(task, snapshot)

    def schedule(self, snapshot):
        self.index.update(snapshot.ready)
        pool = list(self.index.candidates())
        remaining = snapshot.resources
        starts = []
        scores = {task.id: float(self._score(task, snapshot)) for task in pool}
        if any(not isfinite(score) for score in scores.values()):
            raise SchedulingError("Scheduler weights must be finite")
        while pool:
            feasible = [t for t in pool if self.resources.plan([t], remaining) is not None]
            if not feasible:
                break
            frontier = self._frontier(feasible)
            if not frontier:
                raise SchedulingError("Priority comparator produced no maximal candidate")
            chosen = max(frontier, key=lambda t: (scores[t.id], -t.sequence))
            remaining = self.resources.plan([chosen], remaining).remaining
            starts.append(Start(chosen.id, type(self).__name__, scores[chosen.id]))
            pool.remove(chosen)
        wake_at = None
        if pool and self.refresh_interval is not None:
            wake_at = snapshot.now + self.refresh_interval
        return ScheduleDecision(snapshot.revision, tuple(starts), wake_at)


class PartialOrderScheduler(_GreedyScheduler):
    """Respect the declared order; weight breaks ties and orders incomparable candidates."""

    def __init__(self, *, order=None, **kwargs):
        super().__init__(**kwargs)
        self.order = order if order is not None else NumericPriorityOrder()

    def _frontier(self, feasible):
        # Compare each item to itself too, validating singleton numeric priorities.
        for task in feasible:
            if self.order.compare(task.priority, task.priority) is not Ordering.EQUIVALENT:
                raise SchedulingError("A priority must be equivalent to itself")
        return [
            task
            for task in feasible
            if not any(
                self.order.compare(other.priority, task.priority) is Ordering.HIGHER
                for other in feasible
                if other.id != task.id
            )
        ]


class WeightedScheduler(_GreedyScheduler):
    """Numeric priority is a preference: live weights may reverse its ordering."""

    def __init__(self, *, priority_scale=1.0, **kwargs):
        super().__init__(**kwargs)
        if not isfinite(priority_scale):
            raise ValueError("priority_scale must be finite")
        self.priority_scale = priority_scale

    def _score(self, task, snapshot):
        NumericPriorityOrder().compare(task.priority, task.priority)
        return self.priority_scale * task.priority + super()._score(task, snapshot)
