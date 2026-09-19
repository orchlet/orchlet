"""Typed local checkpoints and deterministic replay signatures.

Flow controllers are replayed, not pickled. Put side effects in tasks and pass
changing data as explicit inputs. Checkpoint files must come from a trusted run:
Python pickle preserves user-defined result types and can execute code on load.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import pickle
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from os import PathLike
from typing import Any, cast

from .contracts import CheckpointCodec
from .errors import RecoveryError


class PickleCheckpointCodec(CheckpointCodec):
    """Preserve Python result types in trusted, local run directories."""

    def encode(self, value: Any) -> bytes:
        try:
            return pickle.dumps(value, protocol=5)
        except Exception as exc:
            raise RecoveryError(
                "Task result cannot be checkpointed; return a serializable value "
                "or provide a CheckpointCodec"
            ) from exc

    def decode(self, data: bytes) -> Any:
        try:
            return pickle.loads(data)
        except Exception as exc:
            raise RecoveryError("Cannot restore the saved task result") from exc


def callable_identity(value: Any) -> tuple[str, str, str]:
    target = cast(
        Any, value if inspect.isfunction(value) or inspect.isclass(value) else type(value)
    )
    try:
        source = inspect.getsource(target)
    except OSError, TypeError:
        source = ""
    return (
        target.__module__,
        target.__qualname__,
        hashlib.sha256(source.encode()).hexdigest(),
    )


def fingerprint(value: Any) -> str:
    """Compare explicit inputs without address-dependent object reprs."""
    active: set[int] = set()

    def normalize(item: Any) -> Any:
        if item is None or isinstance(item, (str, int, bool, float)):
            return [type(cast(Any, item)).__name__, repr(item)]
        if isinstance(item, bytes):
            return ["bytes", item.hex()]
        if isinstance(item, PathLike):
            return ["path", str(cast(PathLike[str], item))]
        if isinstance(item, Enum):
            return ["enum", callable_identity(type(item)), item.name]
        if callable(item):
            return ["callable", callable_identity(item)]
        if id(item) in active:
            raise RecoveryError("Cyclic inputs cannot be compared during recovery")
        active.add(id(item))
        try:
            if is_dataclass(item) and not isinstance(item, type):
                return [
                    "dataclass",
                    callable_identity(type(item)),
                    [[f.name, normalize(getattr(item, f.name))] for f in fields(item)],
                ]
            if isinstance(item, Mapping):
                values = [
                    [normalize(k), normalize(v)] for k, v in cast(Mapping[Any, Any], item).items()
                ]
                return ["mapping", sorted(values, key=lambda pair: json.dumps(pair[0]))]
            if isinstance(item, (list, tuple)):
                return [
                    type(cast(Any, item)).__name__,
                    [normalize(v) for v in cast(list[Any], item)],
                ]
            if isinstance(item, (set, frozenset)):
                return [
                    type(cast(Any, item)).__name__,
                    sorted(
                        (normalize(v) for v in cast(set[Any], item)),
                        key=json.dumps,
                    ),
                ]
            return ["pickle", hashlib.sha256(pickle.dumps(item, protocol=5)).hexdigest()]
        except RecoveryError:
            raise
        except Exception as exc:
            raise RecoveryError(
                f"Cannot compare inputs of type {type(cast(Any, item)).__qualname__}; "
                "pass serializable data as flow/task inputs"
            ) from exc
        finally:
            active.remove(id(cast(Any, item)))

    data = json.dumps(normalize(value), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(data.encode()).hexdigest()
