import json

from marshmallow import Schema, fields, post_load

from utils.entities import KafkaSource, SchemaFieldType, SourceType


class SourceSchema(Schema):
    address = fields.Str(description="Адрес", required=True, example='kafka:9092')
    topic = fields.Str(description="Название топика", required=True, example='calls')
    schema = fields.Dict(keys=fields.Str(), values=fields.Str(), description="JSON-схема [Поле - тип]",
                         required=True, example=json.dumps({'number': SchemaFieldType.INT.value,
                                                            'call_time': SchemaFieldType.INT.value}, indent=4))

    @post_load
    def make_object(self, data, **kwargs):
        return KafkaSource(source_type=SourceType.KAFKA, **data)


class PostSourceSchema(SourceSchema):
    name = fields.Str(description="Уникальное название", required=True, example='calls')

    @post_load
    def make_object(self, data, **kwargs):
        name = data.pop('name')
        return name, KafkaSource(source_type=SourceType.KAFKA, **data)
