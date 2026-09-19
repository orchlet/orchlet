from __future__ import annotations

from collections.abc import Hashable, Iterable
from math import isfinite
from numbers import Real
from typing import Any

from .contracts import PriorityOrder
from .models import Ordering


class NumericPriorityOrder(PriorityOrder):
    def compare(self, a: Any, b: Any) -> Ordering:
        for value in (a, b):
            if isinstance(value, bool) or not isinstance(value, Real) or not isfinite(value):
                raise ValueError("Numeric priorities must be finite real numbers")
        if a == b:
            return Ordering.EQUIVALENT
        return Ordering.HIGHER if a > b else Ordering.LOWER


class ExplicitPriorityOrder(PriorityOrder):
    """Edges (higher, lower) define a strict partial order, including transitive edges."""

    def __init__(self, edges: Iterable[tuple[Hashable, Hashable]] = ()) -> None:
        adjacency: dict[Hashable, set[Hashable]] = {}
        for higher, lower in edges:
            adjacency.setdefault(higher, set()).add(lower)
            adjacency.setdefault(lower, set())
        self._closure: dict[Hashable, frozenset[Hashable]] = {}

        def visit(node: Hashable, visiting: set[Hashable]) -> frozenset[Hashable]:
            if node in visiting:
                raise ValueError("Priority relations contain a cycle")
            if node not in self._closure:
                reachable: set[Hashable] = set()
                for child in adjacency[node]:
                    reachable.add(child)
                    reachable.update(visit(child, visiting | {node}))
                self._closure[node] = frozenset(reachable)
            return self._closure[node]

        for node in adjacency:
            visit(node, set())

    def compare(self, a: Any, b: Any) -> Ordering:
        if a == b:
            return Ordering.EQUIVALENT
        if b in self._closure.get(a, ()):
            return Ordering.HIGHER
        if a in self._closure.get(b, ()):
            return Ordering.LOWER
        return Ordering.INCOMPARABLE
