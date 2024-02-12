from marshmallow import Schema, fields, post_load

from utils.entities import OperatorType, Deduplicator, Filter, Output, Clone, FieldDeleter, FieldChanger, \
    FieldEnricher, StreamJoiner


class DeduplicatorSchema(Schema):
    source = fields.Str(description="Источник данных", required=True, example='data_with_numbers')
    key = fields.Str(description="Ключ", required=True, example='number')
    time = fields.Int(description="Время (в секундах)", required=True, example='60')

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, Deduplicator(operator_type=OperatorType.DEDUPLICATOR, **data)


class FilterSchema(Schema):
    source = fields.Str(description="Источник данных", required=True, example='data_with_numbers')
    expression = fields.Str(description="Выражение", required=True, example="item['number'] == 100")

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, Filter(operator_type=OperatorType.FILTER, **data)


class OutputSchema(Schema):
    source = fields.Str(description="Источник данных", required=True, example='data_with_numbers')
    sink = fields.Str(description="Выходное хранилище", required=True, example='data_with_numbers')

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, Output(operator_type=OperatorType.OUTPUT, **data)


class CloneSchema(Schema):
    source = fields.Str(description="Источник данных", required=True, example='data_with_numbers')
    clone_name = fields.Str(description="Название копии", required=True, example='clone_data')

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, Clone(operator_type=OperatorType.CLONE, **data)


class FieldDeleterSchema(Schema):
    source = fields.Str(description="Источник данных", required=True, example='data_with_numbers')
    field_name = fields.Str(description="Название поля", required=True, example='number')

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, FieldDeleter(operator_type=OperatorType.FIELD_DELETER, **data)


class FieldChangerSchema(Schema):
    source = fields.Str(description="Источник данных", required=True, example='data_with_numbers')
    field_name = fields.Str(description="Название поля", required=True, example='number')
    value = fields.Str(description="Новое значение (выражение)", required=True, example="item['number'] + 1")

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, FieldChanger(operator_type=OperatorType.FIELD_CHANGER, **data)


class FieldEnricherSchema(Schema):
    source = fields.Str(description="Источник данных", required=True, example='data_with_numbers')
    field_name = fields.Str(description="Название нового поля", required=True, example='add_number')
    search_key = fields.Str(description="Ключ (выражение) для поиска", required=True,
                            example="item['string'].split(':')[0]")

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, FieldEnricher(operator_type=OperatorType.FIELD_ENRICHER, **data)


class StreamJoinerSchema(Schema):
    source = fields.Str(description="Основной источник данных", required=True, example='data_with_numbers')
    second_source = fields.Str(description="Второстепенный источник данных", required=True, example='clone_data')
    first_key = fields.Str(description="Ключ основного источника", required=True, example='number')
    second_key = fields.Str(description="Ключ второстепенного источника", required=True, example='number')
    time = fields.Int(description="Время (в секундах)", required=True, example='60')

    @post_load
    def make_object(self, data, **kwargs):
        source = data.pop('source')
        return source, StreamJoiner(operator_type=OperatorType.STREAM_JOINER, **data)
