"""Stable identities must preserve checkpoints without hiding incompatible replay."""

import asyncio
from collections import Counter
import json
from pathlib import Path
import tempfile
from typing import Any, cast
import unittest

from orchlet import (
    EventLoopRuntime,
    FileArtifactStore,
    FlowContext,
    SubmitOptions,
    TaskHandle,
    flow,
    task,
)
from orchlet.artifacts import read_json
from orchlet.checkpoints import fingerprint
from orchlet.errors import RecoveryError, TaskFailed
from orchlet.policies import AllSettled, BoundedAdmission


class LogicalKeyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.runtimes: list[EventLoopRuntime] = []
        self.loop_errors: list[dict[str, Any]] = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, context: self.loop_errors.append(context)
        )

    def runtime(self, **kwargs: Any) -> EventLoopRuntime:
        runtime = EventLoopRuntime(artifacts=FileArtifactStore(self.directory / "runs"), **kwargs)
        self.runtimes.append(runtime)
        return runtime

    async def asyncTearDown(self) -> None:
        for runtime in self.runtimes:
            await asyncio.wait_for(runtime.aclose(), 5)
        await asyncio.sleep(0)
        self.temporary.cleanup()
        self.assertEqual(self.loop_errors, [], "Keyed submissions leaked an asynchronous error")

    async def test_task_reordering_reuses_successes_and_retries_only_failed_work(self) -> None:
        order = ["bug/1", "bug%2F1", "bug:2", "bug 3"]
        fail = True
        calls: Counter[str] = Counter()
        submissions: list[dict[str, TaskHandle[str]]] = []

        @task
        async def work(item: str) -> str:
            calls[item] += 1
            if item == "bug:2" and fail:
                raise ValueError("Fix failed")
            return f"fixed {item}"

        @flow
        async def pipeline(ctx: FlowContext) -> list[str | None]:
            handles = {
                item: ctx.submit(work, item, options=SubmitOptions(key=item)) for item in order
            }
            submissions.append(handles)
            outcomes = await ctx.all_settled(handles.values())
            for outcome in outcomes:
                if outcome.error is not None:
                    raise outcome.error
            return [outcome.value for outcome in outcomes]

        first = self.runtime().start(pipeline)
        with self.assertRaises(TaskFailed):
            await first
        fail = False
        order.reverse()
        second = self.runtime().start(pipeline)
        self.assertEqual(second.id, first.id)
        self.assertEqual(await second, [f"fixed {item}" for item in order])
        self.assertEqual(calls, {"bug/1": 1, "bug%2F1": 1, "bug:2": 2, "bug 3": 1})
        self.assertEqual(len({handle.id for handle in submissions[0].values()}), len(order))
        for item in order:
            before, after = submissions[0][item], submissions[1][item]
            self.assertEqual(before.id, after.id)
            self.assertEqual(before.artifacts_dir, after.artifacts_dir)
            self.assertEqual(after.key, item)
            metadata = read_json(after.artifacts_dir / "task.json")
            self.assertEqual(metadata["key"], item)
            self.assertEqual(len(metadata["attempts"]), 2 if item == "bug:2" else 1)
        self.assertTrue(
            (submissions[1]["bug:2"].artifacts_dir / "attempts/002/result.json").is_file()
        )

    async def test_nested_keys_survive_reordering_at_both_levels(self) -> None:
        items = [1, 2]
        stages = ["review", "fix"]
        fail = True
        calls: Counter[tuple[int, str]] = Counter()
        ids: list[dict[tuple[int, str], str]] = []

        @task
        async def work(item: int, stage: str) -> str:
            calls[item, stage] += 1
            return f"{item}:{stage}"

        @flow
        async def branch(ctx: FlowContext, item: int) -> list[str]:
            handles = [
                ctx.with_options(SubmitOptions(key=stage)).submit(work, item, stage)
                for stage in stages
            ]
            for stage, handle in zip(stages, handles):
                ids[-1][item, stage] = handle.id
            return [await handle for handle in handles]

        @flow
        async def pipeline(ctx: FlowContext) -> list[list[str]]:
            ids.append({})
            handles = [
                ctx.with_options(SubmitOptions(key=f"bug:{item}")).subflow(branch, item)
                for item in items
            ]
            for item, handle in zip(items, handles):
                self.assertEqual(handle.key, f"bug:{item}")
                ids[-1][item, "branch"] = handle.id
            results = [await handle for handle in handles]
            if fail:
                raise ValueError("Controller interrupted")
            return results

        with self.assertRaisesRegex(ValueError, "Controller interrupted"):
            await self.runtime().arun(pipeline)
        fail = False
        items.reverse()
        stages.reverse()
        self.assertEqual(
            await self.runtime().arun(pipeline), [["2:fix", "2:review"], ["1:fix", "1:review"]]
        )
        self.assertEqual(ids[0], ids[1])
        self.assertEqual(calls, {(item, stage): 1 for item in items for stage in stages})

    async def test_keyed_nodes_do_not_consume_positional_numbers(self) -> None:
        reverse = False
        calls: Counter[str] = Counter()
        ids: list[dict[str, str]] = []

        @task
        async def work(label: str) -> str:
            calls[label] += 1
            return label

        @flow
        async def branch(ctx: FlowContext, label: str) -> str:
            return await ctx.submit(work, label)

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            current: dict[str, str] = {}
            for keyed in [False, True] if reverse else [True, False]:
                context = ctx.with_options(SubmitOptions(key="task")) if keyed else ctx
                handle = context.submit(work, f"task-{keyed}")
                current[f"task-{keyed}"] = handle.id
                await handle
                context = ctx.with_options(SubmitOptions(key="flow")) if keyed else ctx
                child = context.subflow(branch, f"flow-{keyed}")
                current[f"flow-{keyed}"] = child.id
                await child
            ids.append(current)
            if not reverse:
                raise ValueError("Controller interrupted")

        with self.assertRaisesRegex(ValueError, "Controller interrupted"):
            await self.runtime().arun(pipeline)
        reverse = True
        await self.runtime().arun(pipeline)
        self.assertEqual(ids[0], ids[1])
        self.assertIn("/task-1:", ids[1]["task-False"])
        self.assertIn("/flow-1:", ids[1]["flow-False"])
        self.assertEqual(calls, {"task-True": 1, "task-False": 1, "flow-True": 1, "flow-False": 1})

    async def test_views_preserve_business_keywords_and_leave_other_contexts_unchanged(
        self,
    ) -> None:
        @task
        def work(value: str) -> str:
            return value

        @flow
        async def child(ctx: FlowContext, *, key: int, options: str) -> str:
            handle = ctx.submit(work, f"{key}:{options}")
            self.assertIsNone(handle.key)
            return await handle

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            view = ctx.with_options(SubmitOptions(key="child"))
            self.assertEqual(view.scope_id, ctx.scope_id)
            self.assertEqual(view.run_id, ctx.run_id)
            self.assertEqual(view.artifacts_dir, ctx.artifacts_dir)
            self.assertEqual(await view.subflow(child, key=7, options="business"), "7:business")
            overridden = view.submit(work, "override", options=SubmitOptions(key="override"))
            self.assertEqual(overridden.key, "override")
            self.assertEqual(await overridden, "override")
            cleared = view.submit(work, "clear", options=SubmitOptions())
            self.assertIsNone(cleared.key)
            self.assertEqual(await cleared, "clear")
            replaced = view.with_options(SubmitOptions(key="replace")).submit(work, "replace")
            self.assertEqual(replaced.key, "replace")
            await replaced
            ordinary = ctx.submit(work, "ordinary")
            self.assertIsNone(ordinary.key)
            self.assertEqual(await ordinary, "ordinary")

        await self.runtime().arun(pipeline)

    async def test_keys_are_unique_across_definitions_and_node_kinds_even_after_completion(
        self,
    ) -> None:
        @task
        def work() -> int:
            return 1

        @flow
        async def branch(ctx: FlowContext) -> int:
            return 2

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            view = ctx.with_options(SubmitOptions(key="one"))
            original = view.submit(work)
            with self.assertRaisesRegex(ValueError, "Duplicate submission key"):
                view.submit(work.options(name="other"))
            with self.assertRaisesRegex(ValueError, "Duplicate submission key"):
                view.subflow(branch)
            self.assertEqual(await original, 1)
            with self.assertRaisesRegex(ValueError, "Duplicate submission key"):
                view.submit(work)
            other = ctx.with_options(SubmitOptions(key="two"))
            self.assertEqual(await other.subflow(branch), 2)
            with self.assertRaisesRegex(ValueError, "Duplicate submission key"):
                other.subflow(branch)
            with self.assertRaisesRegex(ValueError, "Duplicate submission key"):
                other.submit(work)

        await self.runtime().arun(pipeline)

    def test_invalid_keys_are_rejected(self) -> None:
        values: list[object] = ["", 1, True, [], {}]
        for value in values:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "nonempty strings"):
                SubmitOptions(key=cast(str, value))

    async def test_keyed_asubmit_preserves_backpressure_and_dependency_policies(self) -> None:
        finished = False

        @task
        async def fail() -> None:
            nonlocal finished
            await asyncio.sleep(0.01)
            finished = True
            raise ValueError("Expected failure")

        @task
        async def cleanup() -> str:
            self.assertTrue(finished)
            return "cleaned"

        @flow
        async def child(ctx: FlowContext) -> None:
            pass

        @flow
        async def pipeline(ctx: FlowContext) -> str:
            first = ctx.submit(fail, options=SubmitOptions(key="fail"))
            view = ctx.with_options(
                SubmitOptions(key="cleanup", after=(first,), dependency_policy=AllSettled())
            )
            with self.assertRaisesRegex(ValueError, "only key"):
                view.subflow(child)
            handle = await view.asubmit(cleanup)
            self.assertEqual(handle.key, "cleanup")
            self.assertTrue(finished)
            return await handle

        self.assertEqual(
            await self.runtime(admission=BoundedAdmission(1)).arun(pipeline), "cleaned"
        )

    async def test_recovery_rejects_changed_inputs_for_tasks_and_subflows(self) -> None:
        for kind in ("task", "flow"):
            with self.subTest(kind=kind):
                supplied = 1
                entered: list[int] = []

                @task
                async def work(value: int) -> int:
                    entered.append(value)
                    return value

                @flow
                async def branch(ctx: FlowContext, value: int) -> int:
                    return await ctx.submit(work, value)

                @flow
                async def pipeline(ctx: FlowContext) -> None:
                    view = ctx.with_options(SubmitOptions(key="stable"))
                    if kind == "task":
                        await view.submit(work, supplied)
                    else:
                        await view.subflow(branch, supplied)
                    raise ValueError("Controller interrupted")

                with self.assertRaisesRegex(ValueError, "Controller interrupted"):
                    await self.runtime(run_key=kind).arun(pipeline)
                supplied = 2
                with self.assertRaises(RecoveryError):
                    await asyncio.wait_for(self.runtime(run_key=kind).arun(pipeline), 5)
                self.assertEqual(entered, [1])

    async def test_recovery_rejects_renamed_or_replaced_task_definitions(self) -> None:
        for change in ("name", "implementation"):
            with self.subTest(change=change):
                entered: list[str] = []

                @task
                async def work() -> int:
                    entered.append("original")
                    return 1

                @task
                async def replacement() -> int:
                    entered.append("replacement")
                    return 2

                definition = work

                @flow
                async def pipeline(ctx: FlowContext) -> None:
                    await ctx.submit(definition, options=SubmitOptions(key="stable"))
                    raise ValueError("Controller interrupted")

                with self.assertRaisesRegex(ValueError, "Controller interrupted"):
                    await self.runtime(run_key=change).arun(pipeline)
                definition = (
                    work.options(name="renamed")
                    if change == "name"
                    else replacement.options(name=work.name)
                )
                with self.assertRaises(RecoveryError):
                    await asyncio.wait_for(self.runtime(run_key=change).arun(pipeline), 5)
                self.assertEqual(entered, ["original"])

    async def test_recovery_rejects_reusing_a_task_key_for_a_subflow(self) -> None:
        use_flow = False
        entered: list[str] = []

        @task
        async def work() -> int:
            entered.append("task")
            return 1

        @flow
        async def branch(ctx: FlowContext) -> int:
            entered.append("flow")
            return 2

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            view = ctx.with_options(SubmitOptions(key="same"))
            if use_flow:
                await view.subflow(branch)
            else:
                await view.submit(work)
            raise ValueError("Controller interrupted")

        with self.assertRaisesRegex(ValueError, "Controller interrupted"):
            await self.runtime().arun(pipeline)
        use_flow = True
        with self.assertRaisesRegex(RecoveryError, "Submission identity changed"):
            await asyncio.wait_for(self.runtime().arun(pipeline), 5)
        self.assertEqual(entered, ["task"])

    async def test_a_bound_key_can_stabilize_the_batch_itself(self) -> None:
        order = ["first", "second"]
        fail = True
        calls: Counter[int] = Counter()

        @task
        async def work(value: int) -> int:
            calls[value] += 1
            return value

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            return await ctx.submit(work, value, options=SubmitOptions(key="work"))

        @flow
        async def pipeline(ctx: FlowContext) -> dict[str, list[int]]:
            results: dict[str, list[int]] = {}
            for name in order:
                view = ctx.with_options(SubmitOptions(key=name))
                items = [1, 2] if name == "first" else [3, 4]
                results[name] = await view.map_flows(branch, items, key=str)
            if fail:
                raise ValueError("Controller interrupted")
            return results

        with self.assertRaisesRegex(ValueError, "Controller interrupted"):
            await self.runtime().arun(pipeline)
        fail = False
        order.reverse()
        self.assertEqual(await self.runtime().arun(pipeline), {"second": [3, 4], "first": [1, 2]})
        self.assertEqual(calls, {1: 1, 2: 1, 3: 1, 4: 1})

    async def test_existing_batch_slot_format_can_be_resumed(self) -> None:
        fail = True
        items = [1, 2]
        calls: Counter[int] = Counter()

        @task
        async def work(value: int) -> int:
            calls[value] += 1
            return value

        @flow
        async def branch(ctx: FlowContext, value: int) -> int:
            return await ctx.submit(work, value)

        @flow
        async def pipeline(ctx: FlowContext) -> list[int]:
            values = await ctx.map_flows(branch, items, key=str)
            if fail:
                raise ValueError("Controller interrupted")
            return values

        first = self.runtime().start(pipeline)
        with self.assertRaisesRegex(ValueError, "Controller interrupted"):
            await first
        # Before ordinary nodes supported keys, batches stored slots by the ID prefix.
        slots = first.artifacts_dir / "slots"
        converted = 0
        for path in slots.glob("*.json"):
            metadata = read_json(path)
            if metadata.get("key") is not None:
                node_id: str = metadata["id"]
                legacy = slots / f"{fingerprint(node_id.rsplit(':', 1)[0])}.json"
                legacy.write_text(json.dumps({"id": node_id}), encoding="utf-8")
                path.unlink()
                converted += 1
        self.assertEqual(converted, 2)
        fail = False
        items.reverse()
        self.assertEqual(await self.runtime().arun(pipeline), [2, 1])
        self.assertEqual(calls, {1: 1, 2: 1})

    async def test_external_submissions_share_key_semantics(self) -> None:
        @task
        def work(value: int) -> int:
            return value * 2

        @flow
        async def pipeline(ctx: FlowContext) -> None:
            pass

        run = self.runtime().start(pipeline, keep_open=True)
        handle = run.submit(work, 2, options=SubmitOptions(key="external/item"))
        self.assertEqual(await handle, 4)
        self.assertEqual(handle.key, "external/item")
        with self.assertRaisesRegex(ValueError, "Duplicate submission key"):
            run.submit(work, 3, options=SubmitOptions(key="external/item"))
        await run.close_inputs()
        await run
