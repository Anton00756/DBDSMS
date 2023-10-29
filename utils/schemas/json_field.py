from typing import Any

import marshmallow.exceptions


class JSON(marshmallow.fields.Field):
    def __init__(self, fields_to_delete=None, **kwargs):
        super().__init__(**kwargs)
        self.fields = fields_to_delete if fields_to_delete else []

    def _serialize(self, value: Any, attr: str, obj: str, **kwargs):
        result = value.__dict__
        for field in self.fields:
            if field in result:
                del result[field]
        return result

    def _deserialize(self, value, attr=None, data=None, **kwargs):
        return value
