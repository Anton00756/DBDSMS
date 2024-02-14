import json

from marshmallow import Schema, fields, post_load

from utils.entities import GreenplumSink, KafkaSink, MinioSink, SinkType


class KafkaSinkSchema(Schema):
    address = fields.Str(description="Адрес", required=True, example='kafka:9092')
    topic = fields.Str(description="Топик", required=True, example='person_calls')

    @post_load
    def make_object(self, data, **kwargs):
        return KafkaSink(sink_type=SinkType.KAFKA, **data)


class PostKafkaSinkSchema(KafkaSinkSchema):
    name = fields.Str(description="Уникальное название", required=True, example='kafka_sink')

    @post_load
    def make_object(self, data, **kwargs):
        name = data.pop('name')
        return name, KafkaSink(sink_type=SinkType.KAFKA, **data)


class GreenplumSinkSchema(Schema):
    address = fields.Str(description="Адрес", required=True, example='greenplum:5432')
    database = fields.Str(description="База данных", required=True, example='processed_data_db')
    table = fields.Str(description="Таблица", required=True, example='person_and_call_time')
    init_sql = fields.Str(description="SQL-скрипт для инициализации", required=True,
                          example='create table if not exists person_and_call_time (message person, call_time float)')
    schema = fields.Dict(keys=fields.Str(), values=fields.Str(), description="Схема [JSON-поле - поле в базе]",
                         required=True, example=json.dumps({'person': 'person', 'time': 'time'}, indent=4))

    @post_load
    def make_object(self, data, **kwargs):
        return GreenplumSink(sink_type=SinkType.GREENPLUM, **data)


class PostGreenplumSinkSchema(GreenplumSinkSchema):
    name = fields.Str(description="Уникальное название", required=True, example='greenplum_sink')

    @post_load
    def make_object(self, data, **kwargs):
        name = data.pop('name')
        return name, GreenplumSink(sink_type=SinkType.GREENPLUM, **data)


class MinioSinkSchema(Schema):
    address = fields.Str(description="Название сегмента", required=True, example='results')
    title_field = fields.Str(description="Название сегмента", required=False, example='number')

    @post_load
    def make_object(self, data, **kwargs):
        return MinioSink(sink_type=SinkType.MINIO, **data)


class PostMinioSinkSchema(MinioSinkSchema):
    name = fields.Str(description="Уникальное название", required=True, example='minio_sink')

    @post_load
    def make_object(self, data, **kwargs):
        name = data.pop('name')
        return name, MinioSink(sink_type=SinkType.MINIO, **data)
