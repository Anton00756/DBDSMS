from marshmallow import Schema, fields


class JobStatusSchema(Schema):
    status = fields.Str(description="Статус", required=True, example='running')


class ErrorSchema(Schema):
    result = fields.Str(description="Описание ошибки", required=True, example='Пропущено поле "field"')


class InputSchema(Schema):
    number = fields.Int(description="Число", required=True, example=5)
    power = fields.Int(description="Степень", required=True, example=2)


class OutputSchema(Schema):
    result = fields.Int(description="Результат", required=True, example=25)

