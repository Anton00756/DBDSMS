import enum
from abc import ABC


class SchemaFieldType(enum.Enum):
    BOOL = 'bool'  # BOOLEAN
    INT = 'int'  # INT
    DOUBLE = 'double'  # DOUBLE
    STRING = 'string'  # STRING

    def to_json(self):
        return self.value

    @staticmethod
    def get_python_type_by_field_type(field_type):
        return FIELD_TYPE_TO_PYTHON[field_type]

    @staticmethod
    def convert_type(in_type, value):
        try:
            return PYTHON_TYPE_TO_FIELD.get(type(SchemaFieldType.get_python_type_by_field_type(in_type)(value)))
        except ValueError:
            return SchemaFieldType.STRING

    def get_example_value(self):
        if self == SchemaFieldType.BOOL:
            return True
        if self == SchemaFieldType.INT:
            return 0
        if self == SchemaFieldType.DOUBLE:
            return 0.0
        if self == SchemaFieldType.STRING:
            return 'string'


FIELD_TYPE_TO_PYTHON = {
    SchemaFieldType.BOOL: bool, SchemaFieldType.INT: int, SchemaFieldType.DOUBLE: float, SchemaFieldType.STRING: str
}
PYTHON_TYPE_TO_FIELD = {
    bool: SchemaFieldType.BOOL, int: SchemaFieldType.INT, float: SchemaFieldType.DOUBLE, str: SchemaFieldType.STRING
}


class SourceType(enum.Enum):
    KAFKA = 'kafka'
    CLONE = 'clone'


class SinkType(enum.Enum):
    GREENPLUM = 'greenplum'
    KAFKA = 'kafka'
    MINIO = 'minio'


class OperatorType(enum.Enum):
    DEDUPLICATOR = 'deduplicator'
    FILTER = 'filter'
    OUTPUT = 'output'
    CLONE = 'clone'
    FIELD_DELETER = 'field_deleter'
    FIELD_CHANGER = 'field_changer'
    FIELD_ENRICHER = 'field_enricher'
    STREAM_JOINER = 'stream_joiner'
    STREAM_JOINER_PLUG = 'stream_joiner_plug'


class BaseEntity(ABC):
    def to_json(self) -> dict:
        result = {}
        for key, value in vars(self).items():
            if hasattr(value, 'to_json'):
                result[key] = value.to_json()
            elif isinstance(value, list):
                result[key] = [item.to_json() if hasattr(item, 'to_json') else item for item in value]
            elif isinstance(value, dict):
                if isinstance(next((element for element in value.values()), None), list):
                    result[key] = {key: [element.to_json() if hasattr(element, 'to_json') else element
                                         for element in item] for key, item in value.items()}
                else:
                    result[key] = {key: item.to_json() if hasattr(item, 'to_json') else item
                                   for key, item in value.items()}
            elif isinstance(value, enum.Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result
