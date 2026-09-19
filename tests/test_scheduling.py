import unittest
from collections.abc import Sequence
from typing import Any

from orchlet import Scheduler
from orchlet.errors import RevisionConflict, SchedulingError
from orchlet.models import Ordering, ScheduleDecision, ScheduleSnapshot, TaskState, TaskView
from orchlet.priorities import ExplicitPriorityOrder, NumericPriorityOrder
from orchlet.resources import TokenResourceAllocator
from orchlet.schedulers import PartialOrderScheduler, WeightedScheduler
from orchlet.state import MemoryStateStore
from orchlet.weights import AgingWeight, FunctionWeight, MetricWeight


def node(name: str, priority: int | str = 0, sequence: int = 0, **kwargs: Any) -> TaskView:
    return TaskView(
        name, "run", "scope", name, TaskState.READY, priority=priority, sequence=sequence, **kwargs
    )


def choose(
    scheduler: Scheduler,
    tasks: Sequence[TaskView],
    *,
    slots: int = 1,
    now: float = 10,
    **capacities: float,
) -> ScheduleDecision:
    allocator = TokenResourceAllocator(slots, **capacities)
    scheduler.bind_resources(allocator)
    snapshot = ScheduleSnapshot(0, now, tuple(tasks), (), allocator.initial())
    return scheduler.schedule(snapshot)


class SchedulingTests(unittest.TestCase):
    def test_numeric_order(self):
        order = NumericPriorityOrder()
        self.assertEqual(order.compare(4, 3), Ordering.HIGHER)
        self.assertEqual(order.compare(3, 4), Ordering.LOWER)
        self.assertEqual(order.compare(4, 4), Ordering.EQUIVALENT)
        for invalid in (float("nan"), float("inf"), True, "high"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                order.compare(invalid, invalid)

    def test_partial_order_transitivity_and_incomparability(self):
        order = ExplicitPriorityOrder([("high", "medium"), ("medium", "low")])
        self.assertEqual(order.compare("high", "low"), Ordering.HIGHER)
        self.assertEqual(order.compare("low", "high"), Ordering.LOWER)
        self.assertEqual(order.compare("sideways", "high"), Ordering.INCOMPARABLE)
        for edges in ([(1, 1)], [(1, 2), (2, 3), (3, 1)]):
            with self.assertRaises(ValueError):
                ExplicitPriorityOrder(edges)

    def test_static_order_is_preserved_by_partial_order_scheduler(self):
        tasks = [node("low", 0, metrics={"weight": 100}), node("high", 10)]
        decision = choose(PartialOrderScheduler(weights=MetricWeight("weight")), tasks)
        self.assertEqual(decision.starts[0].task_id, "high")

    def test_weighted_scheduler_can_reverse_numeric_priority(self):
        tasks = [node("low", 0, metrics={"weight": 100}), node("high", 10)]
        decision = choose(WeightedScheduler(weights=MetricWeight("weight")), tasks)
        self.assertEqual(decision.starts[0].task_id, "low")

    def test_weights_order_incomparable_priorities(self):
        order = ExplicitPriorityOrder([("high", "low")])
        tasks = [node("high", "high"), node("sideways", "sideways", metrics={"w": 2})]
        decision = choose(PartialOrderScheduler(order=order, weights=MetricWeight("w")), tasks)
        self.assertEqual(decision.starts[0].task_id, "sideways")

    def test_fifo_ties(self):
        decision = choose(
            PartialOrderScheduler(), [node("later", sequence=2), node("earlier", sequence=1)]
        )
        self.assertEqual(decision.starts[0].task_id, "earlier")

    def test_batch_respects_multidimensional_resources(self):
        tasks = [
            node("gpu-a", 3, resources={"gpu": 1}),
            node("gpu-b", 2, resources={"gpu": 1}),
            node("cpu", 1),
        ]
        decision = choose(PartialOrderScheduler(), tasks, slots=3, gpu=1)
        self.assertEqual([s.task_id for s in decision.starts], ["gpu-a", "cpu"])

    def test_infeasible_high_priority_does_not_block_cpu(self):
        decision = choose(
            PartialOrderScheduler(), [node("gpu", 100, resources={"gpu": 1}), node("cpu")], gpu=0
        )
        self.assertEqual([s.task_id for s in decision.starts], ["cpu"])

    def test_weights_recomputed_for_each_snapshot(self):
        scheduler = WeightedScheduler(weights=AgingWeight())
        # New arrivals do not inherit the long wait of the older task.
        tasks = [node("old", 0, ready_at=0), node("new", 10, ready_at=19)]
        self.assertEqual(choose(scheduler, tasks, now=20).starts[0].task_id, "old")

    def test_invalid_weight_fails_explicitly(self):
        with self.assertRaises(SchedulingError):
            choose(
                WeightedScheduler(weights=FunctionWeight(lambda _task, _snapshot: float("nan"))),
                [node("a")],
            )

    def test_snapshots_freeze_nested_metadata(self):
        original = {"nested": {"items": [1, 2]}}
        task = node("a", metadata=original)
        original["nested"]["items"].append(3)
        self.assertEqual(task.metadata["nested"]["items"], (1, 2))
        with self.assertRaises(TypeError):
            task.metadata["nested"]["new"] = 1

    def test_session_reservations_are_exclusive(self):
        allocator = TokenResourceAllocator(3)
        a = node("a", resources={"session:test": 1})
        b = node("b", resources={"session:test": 1})
        self.assertIsNone(allocator.plan([a, b], allocator.initial()))


class StateTests(unittest.IsolatedAsyncioTestCase):
    async def test_versioned_commits_and_immutable_old_snapshot(self):
        store = MemoryStateStore()
        old = store.snapshot()
        await store.commit(old.revision, {"a": node("a")})
        self.assertNotIn("a", old.tasks)
        self.assertIn("a", store.snapshot().tasks)
        with self.assertRaises(RevisionConflict):
            await store.commit(old.revision, {})
