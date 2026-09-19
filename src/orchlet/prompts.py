from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from .contracts import PromptBuilder
from .models import AttemptContext


class FunctionPromptBuilder(PromptBuilder):
    def __init__(self, include_feedback: bool = True) -> None:
        self.include_feedback = include_feedback

    async def build(
        self,
        function: Callable[..., str | Awaitable[str]],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        attempt: AttemptContext,
    ) -> str:
        if inspect.iscoroutinefunction(function):
            prompt = await function(*args, **kwargs)
        else:
            prompt = await asyncio.to_thread(function, *args, **kwargs)
        if not isinstance(prompt, str):
            raise TypeError("An @agent prompt function must return str")
        if self.include_feedback and attempt.previous_error:
            prompt += (
                f"\n\nPrevious response:\n{attempt.previous_output or ''}"
                f"\n\nPrevious attempt error:\n{attempt.previous_error}"
                "\nCorrect the response while satisfying the original requirements."
            )
        return prompt
