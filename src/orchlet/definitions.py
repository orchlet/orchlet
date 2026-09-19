from __future__ import annotations

import inspect
import math
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass, field, replace
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Literal,
    Protocol,
    TypedDict,
    Unpack,
    cast,
    overload,
)

from typing_extensions import TypeForm

from .contracts import (
    AgentBackend,
    FlowController,
    OutputCodec,
    PromptBuilder,
    ResultValidator,
    RetryPolicy,
    SessionPolicy,
)
from .models import freeze
from .outputs import CheckValidator, CompositeValidator, JsonCodec, TextCodec, TypeValidator
from .prompts import FunctionPromptBuilder
from .sessions import FreshSession

if TYPE_CHECKING:
    from .runners import TaskContext
    from .runtime import FlowContext


class _TaskOptions(TypedDict, total=False):
    name: str
    kind: str
    priority: Any
    resources: Mapping[str, float]
    metadata: Mapping[str, Any]
    retry: RetryPolicy | None
    timeout: float | None
    backend: str | AgentBackend
    codec: OutputCodec[Any] | None
    prompt_builder: PromptBuilder | None
    session: SessionPolicy | None


class _DefinitionOptions(_TaskOptions, total=False):
    context: bool
    validator: ResultValidator[Any] | None


@dataclass(frozen=True)
class TaskDef[**P, T_co]:
    # P describes submitted arguments; T_co describes the validated task result.
    # Agent prompt functions return text even when their task results are structured.
    function: Callable[..., Any]
    name: str
    kind: str = "python"
    priority: Any = 0
    resources: Mapping[str, float] = field(default_factory=lambda: dict[str, float]())
    metadata: Mapping[str, Any] = field(default_factory=lambda: dict[str, Any]())
    retry: RetryPolicy | None = None
    timeout: float | None = None
    validator: ResultValidator[Any] | None = None
    backend: str | AgentBackend = "codex"
    codec: OutputCodec[Any] | None = None
    prompt_builder: PromptBuilder | None = None
    session: SessionPolicy | None = None
    context: bool = False

    def __post_init__(self) -> None:
        if self.timeout is not None and (not math.isfinite(self.timeout) or self.timeout <= 0):
            raise ValueError("timeout must be positive and finite")
        object.__setattr__(self, "resources", freeze(self.resources))
        object.__setattr__(self, "metadata", freeze(self.metadata))
        if not self.name:
            raise ValueError("Task name cannot be empty")

    def options(self, **changes: Unpack[_DefinitionOptions]) -> TaskDef[P, T_co]:
        """Return a new definition; existing task instances retain their declared options."""
        return replace(self, **changes)


class CoroutineFlowController[**P, T_co](FlowController[T_co]):
    def __init__(self, function: Callable[Concatenate[FlowContext, P], Awaitable[T_co]]) -> None:
        if not inspect.iscoroutinefunction(function):
            raise TypeError("A flow must be an async function")
        self.function: Callable[..., Awaitable[T_co]] = function

    async def run(
        self, context: FlowContext, args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> T_co:
        return await self.function(context, *args, **kwargs)


@dataclass(frozen=True)
class FlowDef[**P, T_co]:
    controller: FlowController[T_co]
    name: str


def flow[**P, T](function: Callable[Concatenate[FlowContext, P], Awaitable[T]]) -> FlowDef[P, T]:
    return FlowDef(CoroutineFlowController(function), function.__name__)


def _validator(
    result_type: Any, validate: ResultValidator[Any] | Callable[[Any], object] | None
) -> ResultValidator[Any] | None:
    validators: list[ResultValidator[Any]] = []
    if result_type is not None:
        validators.append(TypeValidator(result_type))
    if validate is not None:
        validators.append(
            cast(ResultValidator[Any], validate)
            if isinstance(validate, ResultValidator)
            else CheckValidator(validate)
        )
    return CompositeValidator(validators) if validators else None


class _TaskDecorator(Protocol):
    @overload
    def __call__[**P, T](
        self, function: Callable[P, Coroutine[Any, Any, T]], /
    ) -> TaskDef[P, T]: ...
    @overload
    def __call__[**P, T](self, function: Callable[P, T], /) -> TaskDef[P, T]: ...


class _ContextTaskDecorator(Protocol):
    @overload
    def __call__[**P, T](
        self, function: Callable[Concatenate[TaskContext, P], Coroutine[Any, Any, T]], /
    ) -> TaskDef[P, T]: ...
    @overload
    def __call__[**P, T](
        self, function: Callable[Concatenate[TaskContext, P], T], /
    ) -> TaskDef[P, T]: ...


class _TypedTaskDecorator[T_co](Protocol):
    def __call__[**P](self, function: Callable[P, Any], /) -> TaskDef[P, T_co]: ...


class _TypedContextTaskDecorator[T_co](Protocol):
    def __call__[**P](
        self, function: Callable[Concatenate[TaskContext, P], Any], /
    ) -> TaskDef[P, T_co]: ...


@overload
def task[**P, T](
    function: Callable[P, Coroutine[Any, Any, T]],
    *,
    result_type: None = None,
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[False] = False,
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, T]: ...
@overload
def task[**P, T](
    function: Callable[P, T],
    *,
    result_type: None = None,
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[False] = False,
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, T]: ...
@overload
def task[**P, T](
    function: Callable[Concatenate[TaskContext, P], Coroutine[Any, Any, T]],
    *,
    result_type: None = None,
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[True],
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, T]: ...
@overload
def task[**P, T](
    function: Callable[Concatenate[TaskContext, P], T],
    *,
    result_type: None = None,
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[True],
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, T]: ...
@overload
def task[**P, T](
    function: Callable[P, Any],
    *,
    result_type: TypeForm[T],
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[False] = False,
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, T]: ...
@overload
def task[**P, T](
    function: Callable[Concatenate[TaskContext, P], Any],
    *,
    result_type: TypeForm[T],
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[True],
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, T]: ...
@overload
def task(
    function: None = None,
    *,
    result_type: None = None,
    validate: ResultValidator[Any] | Callable[[Any], object] | None = None,
    context: Literal[False] = False,
    **options: Unpack[_TaskOptions],
) -> _TaskDecorator: ...
@overload
def task(
    function: None = None,
    *,
    result_type: None = None,
    validate: ResultValidator[Any] | Callable[[Any], object] | None = None,
    context: Literal[True],
    **options: Unpack[_TaskOptions],
) -> _ContextTaskDecorator: ...
@overload
def task[T](
    function: None = None,
    *,
    result_type: TypeForm[T],
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[False] = False,
    **options: Unpack[_TaskOptions],
) -> _TypedTaskDecorator[T]: ...
@overload
def task[T](
    function: None = None,
    *,
    result_type: TypeForm[T],
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    context: Literal[True],
    **options: Unpack[_TaskOptions],
) -> _TypedContextTaskDecorator[T]: ...


def task(
    function: Callable[..., Any] | None = None,
    *,
    result_type: Any = None,
    validate: ResultValidator[Any] | Callable[[Any], object] | None = None,
    context: bool = False,
    **options: Unpack[_TaskOptions],
) -> Any:
    def decorate(fn: Callable[..., Any]) -> TaskDef[..., Any]:
        values: dict[str, Any] = dict(options)
        return TaskDef(
            function=fn,
            name=values.pop("name", fn.__name__),
            context=context,
            validator=_validator(result_type, validate),
            **values,
        )

    return decorate(function) if function is not None else decorate


class _AgentDecorator[T_co](Protocol):
    def __call__[**P](self, function: Callable[P, str | Awaitable[str]], /) -> TaskDef[P, T_co]: ...


@overload
def agent[**P, T](
    function: Callable[P, str | Awaitable[str]],
    *,
    result_type: TypeForm[T],
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, T]: ...
@overload
def agent[**P](
    function: Callable[P, str | Awaitable[str]],
    *,
    result_type: TypeForm[str] = str,
    validate: ResultValidator[str] | Callable[[str], object] | None = None,
    **options: Unpack[_TaskOptions],
) -> TaskDef[P, str]: ...
@overload
def agent[T](
    function: None = None,
    *,
    result_type: TypeForm[T],
    validate: ResultValidator[T] | Callable[[T], object] | None = None,
    **options: Unpack[_TaskOptions],
) -> _AgentDecorator[T]: ...
@overload
def agent(
    function: None = None,
    *,
    result_type: TypeForm[str] = str,
    validate: ResultValidator[str] | Callable[[str], object] | None = None,
    **options: Unpack[_TaskOptions],
) -> _AgentDecorator[str]: ...


def agent(
    function: Callable[..., Any] | None = None,
    *,
    result_type: Any = str,
    validate: ResultValidator[Any] | Callable[[Any], object] | None = None,
    **options: Unpack[_TaskOptions],
) -> Any:
    def decorate(fn: Callable[..., Any]) -> TaskDef[..., Any]:
        defaults: dict[str, Any] = {
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
