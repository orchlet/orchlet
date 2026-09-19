from dataclasses import fields, is_dataclass, replace

from .contracts import InputResolver
from .handles import OutputRef, TaskHandle


class NestedInputResolver(InputResolver):
    """References in lists, tuples, dict values, and dataclass fields become data edges."""

    def _walk(self, value, reference, path):
        if isinstance(value, (TaskHandle, OutputRef)):
            return reference(value)
        compound = isinstance(value, (list, tuple, dict)) or (
            is_dataclass(value) and not isinstance(value, type)
        )
        if not compound:
            return value
        if id(value) in path:
            raise ValueError("Cyclic input containers are unsupported")
        path = path | {id(value)}
        if isinstance(value, dict):
            if any(isinstance(k, (TaskHandle, OutputRef)) for k in value):
                raise TypeError("Result references must be dict values, not keys")
            return {k: self._walk(v, reference, path) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            items = [self._walk(v, reference, path) for v in value]
            return tuple(items) if isinstance(value, tuple) else items
        updates = {
            f.name: self._walk(getattr(value, f.name), reference, path)
            for f in fields(value)
            if f.init
        }
        return replace(value, **updates)

    def references(self, value):
        refs = []

        def collect(ref):
            refs.append(ref)
            return ref

        self._walk(value, collect, set())
        return tuple(refs)

    def capture(self, value):
        def capture_ref(ref):
            return OutputRef(ref.id, ref.run_id) if isinstance(ref, TaskHandle) else ref

        return self._walk(value, capture_ref, set())

    def resolve(self, value, results):
        return self._walk(value, lambda ref: results[ref.task_id], set())
