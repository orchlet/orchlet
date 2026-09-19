from math import isfinite

from .contracts import ResourceAllocator
from .models import AllocationPlan, ResourceSnapshot


def _quantity(value, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise ValueError("Resource quantities must be finite numbers")
    if value < 0 or (positive and value == 0):
        raise ValueError("Invalid resource quantity")
    return float(value)


class TokenResourceAllocator(ResourceAllocator):
    """Atomic vector reservations. Each task consumes at least one global slot."""

    def __init__(self, global_slots=4, **capacities):
        if "slots" in capacities or any(k.startswith("session:") for k in capacities):
            raise ValueError("slots and session:* capacities are managed by the allocator")
        self.capacity = {"slots": _quantity(global_slots, positive=True)}
        self.capacity.update({k: _quantity(v) for k, v in capacities.items()})

    def initial(self):
        return ResourceSnapshot(self.capacity, self.capacity)

    def requirements(self, task):
        requirements = {"slots": 1.0, **task.resources}
        requirements = {key: _quantity(value) for key, value in requirements.items()}
        if requirements["slots"] < 1:
            raise ValueError("Every leaf task must request at least one slot")
        for key, value in requirements.items():
            if key.startswith("session:") and value != 1:
                raise ValueError("Session reservations must be exclusive")
        return requirements

    def plan(self, tasks, resources):
        available, capacity = dict(resources.available), dict(resources.capacity)
        reservations = {}
        for task in tasks:
            if task.id in reservations:
                raise ValueError("Duplicate task in resource plan")
            requirements = self.requirements(task)
            for key, amount in requirements.items():
                if key.startswith("session:"):
                    capacity.setdefault(key, 1.0)
                    available.setdefault(key, 1.0)
                if key not in capacity or amount > available[key]:
                    return None
                available[key] -= amount
            reservations[task.id] = requirements
        return AllocationPlan(reservations, ResourceSnapshot(capacity, available))
