import json

from pydantic import TypeAdapter

from .contracts import OutputCodec, ResultValidator


class TextCodec(OutputCodec):
    def decode(self, raw):
        return raw


class JsonCodec(OutputCodec):
    def decode(self, raw):
        def invalid_constant(value):
            raise ValueError(f"Nonstandard JSON constant: {value}")

        return json.loads(raw, parse_constant=invalid_constant)


class TypeValidator(ResultValidator):
    def __init__(self, annotation, *, strict=False):
        self.adapter = TypeAdapter(annotation)
        self.strict = strict

    def validate(self, value, context):
        return self.adapter.validate_python(value, strict=self.strict)


class CheckValidator(ResultValidator):
    """A check raises on failure (or returns False); its successful return is ignored."""

    def __init__(self, check):
        self.check = check

    def validate(self, value, context):
        if self.check(value) is False:
            raise ValueError("Result failed the supplied check")
        return value


class CompositeValidator(ResultValidator):
    def __init__(self, validators):
        self.validators = tuple(validators)

    def validate(self, value, context):
        for validator in self.validators:
            value = validator.validate(value, context)
        return value
