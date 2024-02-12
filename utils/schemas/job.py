from marshmallow import Schema, fields, post_load
from utils.entities import Settings, Job
from .sources import SourceSchema
from .json_field import JSON


class JobStatusSchema(Schema):
    status = fields.Str(description="Статус", required=True, example='running')


class JobSettingsSchema(Schema):
    parallelism = fields.Int(description="Параллелизм", required=False, example=2)
    min_pause_between_checkpoints = fields.Int(description="Минимальный интервал между чекпоинтами [мс]",
                                               required=False, example=60000)
    checkpoint_interval = fields.Int(description="Интервал между чекпоинтами [мс]", required=False, example=30000)

    @post_load
    def make_object(self, data, **kwargs):
        return Settings(**data)


class JobSchema(Schema):
    settings = fields.Nested(JobSettingsSchema, required=False, description="Настройки выполнения задания",
                             allow_none=True)
    sources = fields.Dict(keys=fields.Str(), values=JSON(), required=True,
                          description="Входные хранилища")
    sinks = fields.Dict(keys=fields.Str(), values=JSON(), required=True, description="Выходные хранилища")
    operators = fields.Dict(keys=fields.Str(), values=fields.List(JSON()), required=True,
                            description="Операторы")


class ErrorSchema(Schema):
    error = fields.Str(description="Описание ошибки", required=True, example='Пропущено поле "field"')
