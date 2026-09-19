from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import Any

from pydantic import TypeAdapter
from typing_extensions import TypeForm

from .contracts import OutputCodec, ResultValidator
from .models import AttemptContext


class TextCodec(OutputCodec[str]):
    def decode(self, raw: str) -> str:
        return raw


class JsonCodec(OutputCodec[Any]):
    def decode(self, raw: str) -> Any:
        def invalid_constant(value: str) -> None:
            raise ValueError(f"Nonstandard JSON constant: {value}")

        return json.loads(raw, parse_constant=invalid_constant)


class TypeValidator[T](ResultValidator[T]):
    def __init__(self, annotation: TypeForm[T], *, strict: bool = False) -> None:
        self.adapter: TypeAdapter[T] = TypeAdapter[T](annotation)
        self.strict = strict

    def validate(self, value: Any, context: AttemptContext) -> T:
        return self.adapter.validate_python(value, strict=self.strict)


class CheckValidator[T](ResultValidator[T]):
    """A check raises on failure (or returns False); its successful return is ignored."""

    def __init__(self, check: Callable[[T], object]) -> None:
        self.check = check

    def validate(self, value: Any, context: AttemptContext) -> T:
        if self.check(value) is False:
            raise ValueError("Result failed the supplied check")
        return value


class CompositeValidator(ResultValidator[Any]):
    def __init__(self, validators: Iterable[ResultValidator[Any]]) -> None:
        self.validators: tuple[ResultValidator[Any], ...] = tuple(validators)

    def validate(self, value: Any, context: AttemptContext) -> Any:
        for validator in self.validators:
            value = validator.validate(value, context)
        return value
