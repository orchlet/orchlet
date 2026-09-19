import asyncio
import inspect

from .contracts import PromptBuilder


class FunctionPromptBuilder(PromptBuilder):
    def __init__(self, include_feedback=True):
        self.include_feedback = include_feedback

    async def build(self, function, args, kwargs, attempt):
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
