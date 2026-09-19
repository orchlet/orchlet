from __future__ import annotations

import inspect
import math
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Mapping

from .contracts import FlowController, ResultValidator
from .models import freeze
from .outputs import CheckValidator, CompositeValidator, JsonCodec, TextCodec, TypeValidator
from .prompts import FunctionPromptBuilder
from .sessions import FreshSession


@dataclass(frozen=True)
class TaskDef:
    function: Callable
    name: str
    kind: str = "python"
    priority: Any = 0
    resources: Mapping[str, float] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    retry: Any = None
    timeout: float | None = None
    validator: Any = None
    backend: Any = "codex"
    codec: Any = None
    prompt_builder: Any = None
    session: Any = None
    context: bool = False

    def __post_init__(self):
        if self.timeout is not None and (not math.isfinite(self.timeout) or self.timeout <= 0):
            raise ValueError("timeout must be positive and finite")
        object.__setattr__(self, "resources", freeze(self.resources))
        object.__setattr__(self, "metadata", freeze(self.metadata))
        if not self.name:
            raise ValueError("Task name cannot be empty")

    def options(self, **changes):
        """Return a new definition; existing task instances retain their declared options."""
        return replace(self, **changes)


class CoroutineFlowController(FlowController):
    def __init__(self, function):
        if not inspect.iscoroutinefunction(function):
            raise TypeError("A flow must be an async function")
        self.function = function

    async def run(self, context, args, kwargs):
        return await self.function(context, *args, **kwargs)


@dataclass(frozen=True)
class FlowDef:
    controller: FlowController
    name: str


def flow(function):
    return FlowDef(CoroutineFlowController(function), function.__name__)


def _validator(result_type, validate):
    validators = []
    if result_type is not None:
        validators.append(TypeValidator(result_type))
    if validate is not None:
        validators.append(
            validate if isinstance(validate, ResultValidator) else CheckValidator(validate)
        )
    return CompositeValidator(validators) if validators else None


def task(function=None, *, result_type=None, validate=None, **options):
    def decorate(fn):
        return TaskDef(
            function=fn,
            name=options.get("name", fn.__name__),
            validator=_validator(result_type, validate),
            **{key: value for key, value in options.items() if key != "name"},
        )

    return decorate(function) if function is not None else decorate


def agent(function=None, *, result_type=str, validate=None, **options):
    def decorate(fn):
        defaults = {
            "kind": "agent",
            "codec": TextCodec() if result_type is str else JsonCodec(),
            "prompt_builder": FunctionPromptBuilder(),
            "session": FreshSession(),
        }
        defaults.update(options)
        return TaskDef(
            function=fn,
            name=defaults.pop("name", fn.__name__),
            validator=_validator(result_type, validate),
            **defaults,
        )

    return decorate(function) if function is not None else decorate
