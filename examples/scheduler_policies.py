"""Compare execution order under hard priorities, weighted priorities, and custom scheduling."""

from collections.abc import Sequence

from orchlet import (
    EventLoopRuntime,
    FlowContext,
    Scheduler,
    TaskHandle,
    WeightModel,
    configure_logging,
    flow,
    get_logger,
    task,
)
from orchlet.contracts import ResourceAllocator
from orchlet.models import ScheduleDecision, ScheduleSnapshot, Start, TaskView
from orchlet.priorities import ExplicitPriorityOrder
from orchlet.schedulers import PartialOrderScheduler, WeightedScheduler

type JobSpec = tuple[str, int | str, int, int]

logger = get_logger("examples.scheduler_policies")


class UrgencyWeight(WeightModel):
    def evaluate(self, task: TaskView, snapshot: ScheduleSnapshot) -> float:
        # Start with metadata estimates; handle.update_metrics can override them at runtime.
        return float(task.metrics.get("urgency", task.metadata.get("urgency", 0)))


class ShortestJobFirst(Scheduler):
    """Replace the scheduling algorithm with selection based on estimated duration."""

    def bind_resources(self, resources: ResourceAllocator) -> None:
        self.resources = resources

    def schedule(self, snapshot: ScheduleSnapshot) -> ScheduleDecision:
        remaining = snapshot.resources
        starts: list[Start] = []
        candidates = sorted(
            snapshot.ready,
            key=lambda candidate: (candidate.metadata["estimated_seconds"], candidate.sequence),
        )
        for candidate in candidates:
            plan = self.resources.plan([candidate], remaining)
            if plan is not None:
                remaining = plan.remaining
                starts.append(Start(candidate.id, reason="shortest estimated duration"))
        return ScheduleDecision(snapshot.revision, starts=tuple(starts))


@flow
async def workload(ctx: FlowContext, specifications: Sequence[JobSpec]) -> list[str]:
    order: list[str] = []

    @task
    async def work(name: str) -> str:
        order.append(name)
        return name

    jobs: list[TaskHandle[str]] = []
    for name, priority, seconds, urgency in specifications:
        definition = work.options(
            name=name,
            priority=priority,
            metadata={"estimated_seconds": seconds, "urgency": urgency},
        )
        jobs.append(ctx.submit(definition, name))
    for job in jobs:
        await job
    return order


def main() -> None:
    numeric_jobs: list[JobSpec] = [
        ("urgent", 10, 8, 0),
        ("fast", 0, 1, 100),
        ("slow", 0, 4, 0),
    ]
    cases: list[tuple[str, Scheduler, Sequence[JobSpec]]] = [
        ("Hard priorities", PartialOrderScheduler(weights=UrgencyWeight()), numeric_jobs),
        ("Weighted priorities", WeightedScheduler(weights=UrgencyWeight()), numeric_jobs),
        ("Custom shortest-job-first policy", ShortestJobFirst(), numeric_jobs),
        (
            "Partial order (urgent > batch; interactive is incomparable)",
            PartialOrderScheduler(
                order=ExplicitPriorityOrder([("urgent", "batch")]),
                weights=UrgencyWeight(),
            ),
            [
                ("batch", "batch", 4, 1000),
                ("urgent", "urgent", 8, 0),
                ("interactive", "interactive", 1, 30),
            ],
        ),
    ]
    for label, scheduler, specifications in cases:
        runtime = EventLoopRuntime(concurrency=1, scheduler=scheduler)
        order = runtime.run(workload, specifications)
        logger.info(f"{label}: {' -> '.join(order)}")
        # The journal records selection reasons to help compare policies.
        for event in runtime.journal.read():
            if event.kind == "task_started":
                assert event.task_id is not None
                name = event.task_id.rsplit(":", 1)[-1]
                logger.info(f"  {name}: reason={event.data['reason']}, score={event.data['score']}")


if __name__ == "__main__":
    configure_logging()
    main()
