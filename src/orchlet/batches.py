"""Subflow batch coordination; runtime commands own persistence and cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from ._bridges import RuntimeBridge
from .checkpoints import callable_identity, fingerprint
from .contracts import BatchFailurePolicy, FlowController
from .definitions import CoroutineFlowController, FlowDef
from .errors import BatchFailed, RunClosedError, TaskCancelled, TaskFailed
from .handles import FlowHandle, await_shared, observe_handles
from .models import BatchDecision, BatchFailure, BatchSnapshot, Outcome

if TYPE_CHECKING:
    from .runtime import FlowContext


def error_metadata(error: BaseException | None) -> dict[str, object] | None:
    """Keep diagnostic causes without trying to deserialize arbitrary exceptions."""
    if error is None:
        return None
    causes: list[dict[str, str]] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        causes.append(
            {
                "type": f"{type(current).__module__}.{type(current).__qualname__}",
                "message": str(current),
            }
        )
        current = current.__cause__
    return {"causes": causes}


def failed_task(error: BaseException) -> str | None:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        if isinstance(current, TaskFailed):
            return current.task_id
        seen.add(id(current))
        current = current.__cause__
    return None


@dataclass(frozen=True)
class _Member[T]:
    index: int
    key: str
    handle: FlowHandle[T]


class BatchController[I, T](FlowController[list[Outcome[T]]]):
    """An internal coordinator shared by the two statically typed mapping APIs."""

    def __init__(
        self,
        bridge: RuntimeBridge,
        definition: FlowDef[[I], T],
        iterable: Iterable[I],
        key: Callable[[I], str] | None,
        max_in_flight: int | None,
        failure: BatchFailurePolicy,
        collect_errors: bool,
    ) -> None:
        if max_in_flight is not None and (
            isinstance(max_in_flight, bool)
            or not isinstance(cast(object, max_in_flight), int)
            or max_in_flight < 1
        ):
            raise ValueError("max_in_flight must be None or a positive integer")
        self.bridge = bridge
        self.definition = definition
        self.iterable = iterable
        self.key = key
        self.max_in_flight = max_in_flight
        self.failure = failure
        self.collect_errors = collect_errors

    async def run(
        self, context: FlowContext, args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> list[Outcome[T]]:
        batch_id = context.scope_id
        pending: dict[asyncio.Future[T], _Member[T]] = {}
        outcomes: list[Outcome[T] | None] = []
        failures: list[BatchFailure] = []
        keys: set[str] = set()
        cancelled: set[str] = set()
        exhausted = stopped = opened = False

        async def record(kind: str, **data: object) -> None:
            await self.bridge.command(
                "batch_event", context.run_id, batch_id, {"kind": kind, **data}
            )

        async def cancel_pending() -> None:
            handles = [member.handle for future, member in pending.items() if not future.done()]
            cancelled.update(handle.id for handle in handles)
            # Request cancellation of every member before waiting for any cleanup.
            await asyncio.gather(*(handle.cancel() for handle in handles), return_exceptions=True)

        controller = self.definition.controller
        function = (
            controller.function if isinstance(controller, CoroutineFlowController) else controller
        )
        signature = fingerprint(
            (
                self.definition.name,
                callable_identity(function),
                self.key,
                self.max_in_flight,
                callable_identity(self.failure),
                self.collect_errors,
            )
        )
        try:
            await record(
                "started",
                signature=signature,
                flow=self.definition.name,
                max_in_flight=self.max_in_flight,
                policy=type(self.failure).__qualname__,
                collect_errors=self.collect_errors,
            )
            opened = True
            iterator = iter(self.iterable)
            while pending or not (exhausted or stopped):
                ready = sorted(
                    (member for future, member in pending.items() if future.done()),
                    key=lambda member: member.index,
                )
                for member in ready:
                    future = cast(
                        asyncio.Future[T], self.bridge.completion(member.handle.id).future
                    )
                    del pending[future]
                    try:
                        outcome = Outcome(member.handle.id, value=future.result(), key=member.key)
                    except Exception as error:
                        outcome = Outcome[T](member.handle.id, error=error, key=member.key)
                        if not (member.handle.id in cancelled and isinstance(error, TaskCancelled)):
                            failures.append(
                                BatchFailure(
                                    member.key, member.handle.id, error, failed_task(error)
                                )
                            )
                    outcomes[member.index] = outcome
                    await record(
                        "member_finished",
                        key=member.key,
                        status="succeeded" if outcome.succeeded else "failed",
                        error=error_metadata(outcome.error),
                        failed_task_id=failed_task(outcome.error)
                        if outcome.error is not None
                        else None,
                    )
                if ready:
                    decision = self.failure.decide(
                        BatchSnapshot(
                            batch_id, len(outcomes), len(outcomes) - len(pending), tuple(failures)
                        )
                    )
                    if not isinstance(cast(object, decision), BatchDecision):
                        raise TypeError("A batch policy must return a BatchDecision")
                    stopped |= decision != BatchDecision.CONTINUE
                    if decision == BatchDecision.CANCEL:
                        await cancel_pending()

                if not (exhausted or stopped) and (
                    self.max_in_flight is None or len(pending) < self.max_in_flight
                ):
                    try:
                        item = next(iterator)
                    except StopIteration:
                        exhausted = True
                        await record("input_exhausted")
                        continue
                    member_key = str(len(outcomes)) if self.key is None else self.key(item)
                    if not isinstance(cast(object, member_key), str) or not member_key:
                        raise ValueError("Batch member keys must be nonempty strings")
                    if member_key in keys:
                        raise ValueError(f"Duplicate batch member key: {member_key}")
                    keys.add(member_key)
                    handle = self.bridge.subflow(
                        context.run_id, batch_id, self.definition, (item,), {}, key=member_key
                    )
                    observe_handles(self.bridge, context.run_id, [handle])
                    future = cast(asyncio.Future[T], self.bridge.completion(handle.id).future)
                    pending[future] = _Member(len(outcomes), member_key, handle)
                    outcomes.append(None)
                    await record(
                        "member_submitted",
                        key=member_key,
                        flow_id=handle.id,
                        index=len(outcomes) - 1,
                    )
                    continue
                if pending:
                    await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        except BaseException as error:
            await cancel_pending()
            await asyncio.gather(
                *(await_shared(future) for future in pending), return_exceptions=True
            )
            if opened:
                try:
                    await record(
                        "finished",
                        status="cancelled"
                        if isinstance(error, asyncio.CancelledError)
                        else "failed",
                        exhausted=False,
                        error=error_metadata(error),
                    )
                except RunClosedError:
                    pass  # Runtime abort already persists the run failure.
            raise

        # Every admitted member has settled. None is a valid value inside an Outcome.
        results = cast(list[Outcome[T]], outcomes)
        aggregate = (
            BatchFailed(batch_id, failures) if failures and not self.collect_errors else None
        )
        await record(
            "finished",
            status="failed" if aggregate is not None else "settled",
            exhausted=exhausted,
            error=error_metadata(aggregate),
        )
        if aggregate is not None:
            raise aggregate
        return results
