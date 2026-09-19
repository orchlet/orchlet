from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import fields, is_dataclass, replace
from typing import Any, cast

from .contracts import InputResolver
from .handles import OutputRef, TaskHandle


class NestedInputResolver(InputResolver):
    """References in lists, tuples, dict values, and dataclass fields become data edges."""

    def _walk(
        self,
        value: Any,
        reference: Callable[[TaskHandle[Any] | OutputRef[Any]], Any],
        path: set[int],
    ) -> Any:
        if isinstance(value, (TaskHandle, OutputRef)):
            return reference(cast(TaskHandle[Any] | OutputRef[Any], value))
        compound = isinstance(value, (list, tuple, dict)) or (
            is_dataclass(value) and not isinstance(value, type)
        )
        value = cast(Any, value)
        if not compound:
            return value
        if id(value) in path:
            raise ValueError("Cyclic input containers are unsupported")
        path = path | {id(value)}
        if isinstance(value, dict):
            if any(isinstance(k, (TaskHandle, OutputRef)) for k in cast(dict[Any, Any], value)):
                raise TypeError("Result references must be dict values, not keys")
            return {
                k: self._walk(v, reference, path) for k, v in cast(dict[Any, Any], value).items()
            }
        if isinstance(value, (list, tuple)):
            items = [
                self._walk(v, reference, path) for v in cast(list[Any] | tuple[Any, ...], value)
            ]
            return tuple(items) if isinstance(value, tuple) else items
        assert is_dataclass(value) and not isinstance(value, type)
        updates = {
            f.name: self._walk(getattr(value, f.name), reference, path)
            for f in fields(value)
            if f.init
        }
        return replace(value, **updates)

    def references(self, value: Any) -> tuple[TaskHandle[Any] | OutputRef[Any], ...]:
        refs: list[TaskHandle[Any] | OutputRef[Any]] = []

        def collect(ref: TaskHandle[Any] | OutputRef[Any]) -> TaskHandle[Any] | OutputRef[Any]:
            refs.append(ref)
            return ref

        self._walk(value, collect, set())
        return tuple(refs)

    def capture(self, value: Any) -> Any:
        def capture_ref(ref: TaskHandle[Any] | OutputRef[Any]) -> OutputRef[Any]:
            return OutputRef[Any](ref.id, ref.run_id) if isinstance(ref, TaskHandle) else ref

        return self._walk(value, capture_ref, set())

    def resolve(self, value: Any, results: Mapping[str, Any]) -> Any:
        def resolve_ref(ref: TaskHandle[Any] | OutputRef[Any]) -> Any:
            return results[ref.id if isinstance(ref, TaskHandle) else ref.task_id]

        return self._walk(value, resolve_ref, set())
