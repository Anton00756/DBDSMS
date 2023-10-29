import json

from marshmallow import Schema, fields, post_load
from utils.entities import Source


class SourceSchema(Schema):
    address = fields.Str(description="Адрес", required=True, example='kafka:9092')
    topic = fields.Str(description="Название топика", required=True, example='data_with_numbers')
    schema = fields.Dict(keys=fields.Str(), values=fields.Str(), description="JSON-схема [Поле - тип]", required=True,
                         example=json.dumps({'number': 'int', 'string': 'string', 'string2': 'string'}, indent=4))

    @post_load
    def make_object(self, data, **kwargs):
        return Source(**data)


class PostSourceSchema(SourceSchema):
    name = fields.Str(description="Уникальное название", required=True, example='data_with_numbers')

    @post_load
    def make_object(self, data, **kwargs):
        name = data.pop('name')
        return name, Source(**data)
