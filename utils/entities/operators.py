import sys
from abc import abstractmethod
from dataclasses import dataclass
from typing import Dict, Optional

from .base import BaseEntity, OperatorType, SchemaFieldType

IGNORE_EXCEPTIONS: Optional[list] = None


@dataclass
class Operator(BaseEntity):
    operator_type: OperatorType = None

    @abstractmethod
    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        pass

    def __post_init__(self):
        self.operator_type = OperatorType(self.operator_type)


@dataclass
class Deduplicator(Operator):
    key: str = None
    time: int = None

    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        if self.key not in schema:
            raise ValueError(f'Поле "{self.key}" в схеме не найдено!')
        if self.time <= 0:
            raise ValueError('Время должно быть больше 0!')

    def __str__(self):
        return f'Дедупликатор (ключ: "{self.key}", время: {self.time} сек.)'


@dataclass
class Filter(Operator):
    expression: str = None

    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        try:
            eval(self.expression, {'item': {key: value.get_example_value() for key, value in schema.items()}})
        except Exception:
            exception_class, exception_object, _ = sys.exc_info()
            if IGNORE_EXCEPTIONS is None:
                raise ValueError(f'Ошибка при проверке выражения: {exception_class.__name__}: {exception_object}')
            for exception in IGNORE_EXCEPTIONS:
                if exception_class.__name__ == exception:
                    break
            else:
                raise ValueError(f'Ошибка при проверке выражения: {exception_class.__name__}: {exception_object}')

    def __str__(self):
        return f'Фильтр ("{self.expression}")'


@dataclass
class Output(Operator):
    sink: str = None

    def check(self, schema: Dict[str, SchemaFieldType], schema_to_compare: Optional[Dict[str, str]] = None,
              title_name: Optional[str] = None):
        if schema_to_compare is not None:
            if schema.keys() != schema_to_compare.keys():
                raise ValueError(f'Несоответствие полей: {schema.keys()} != {schema_to_compare.keys()}!')
        elif title_name is not None:
            if title_name not in schema:
                raise ValueError(f'Поле "{title_name}" в схеме не найдено!')

    def __str__(self):
        return f'Вывод (выходное хранилище: "{self.sink}")'


@dataclass
class Clone(Operator):
    clone_name: str = None

    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        pass

    def __str__(self):
        return f'Клон (название копии: "{self.clone_name}")'


@dataclass
class FieldDeleter(Operator):
    field_name: str = None

    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        if self.field_name not in schema:
            raise ValueError(f'Поле "{self.field_name}" в схеме не найдено!')

    def __str__(self):
        return f'Удаление поля (название поля: "{self.field_name}")'


@dataclass
class FieldChanger(Operator):
    field_name: str = None
    value: str = None
    new_type: Optional[SchemaFieldType] = None

    def __post_init__(self):
        super().__post_init__()
        if self.new_type is not None:
            self.new_type = SchemaFieldType(self.new_type)

    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        if self.field_name not in schema:
            raise ValueError(f'Поле "{self.field_name}" в схеме не найдено!')
        try:
            eval_result = eval(self.value, {'item': {key: value.get_example_value() for key, value in schema.items()}})
            if self.new_type is None:
                SchemaFieldType.get_python_type_by_field_type(schema[self.field_name])(eval_result)
            else:
                SchemaFieldType.get_python_type_by_field_type(self.new_type)(eval_result)
        except Exception:
            exception_class, exception_object, _ = sys.exc_info()
            if IGNORE_EXCEPTIONS is None:
                raise ValueError(f'Ошибка при проверке выражения: {exception_class.__name__}: {exception_object}')
            for exception in IGNORE_EXCEPTIONS:
                if exception_class.__name__ == exception:
                    break
            else:
                raise ValueError(f'Ошибка при проверке выражения: {exception_class.__name__}: {exception_object}')

    def __str__(self):
        return f'Изменение поля (название поля: "{self.field_name}", новое значение: "{self.value}")'


@dataclass
class FieldEnricher(Operator):
    field_name: str = None
    search_key: str = None

    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        if self.field_name in schema:
            raise ValueError(f'Поле "{self.field_name}" уже существует в схеме!')
        try:
            eval(self.search_key, {'item': {key: value.get_example_value() for key, value in schema.items()}})
        except Exception:
            exception_class, exception_object, _ = sys.exc_info()
            if IGNORE_EXCEPTIONS is None:
                raise ValueError(f'Ошибка при проверке выражения: {exception_class.__name__}: {exception_object}')
            for exception in IGNORE_EXCEPTIONS:
                if exception_class.__name__ == exception:
                    break
            else:
                raise ValueError(f'Ошибка при проверке выражения: {exception_class.__name__}: {exception_object}')

    def __str__(self):
        return f'Обогащение поля (название поля: "{self.field_name}", ключ поиска: "{self.search_key}")'


@dataclass
class StreamJoiner(Operator):
    first_key: str = None
    second_key: str = None
    second_source: str = None
    time: int = None

    def check(self, schema: Dict[str, SchemaFieldType], second_schema: Optional[Dict[str, SchemaFieldType]] = None):
        if self.first_key not in schema:
            raise ValueError(f'Поле "{self.first_key}" в схеме основного источника не найдено!')
        if self.second_key not in second_schema:
            raise ValueError(f'Поле "{self.second_key}" в схеме источника "{self.second_source}" не найдено!')
        if self.time <= 0:
            raise ValueError('Время должно быть больше 0!')

    def __str__(self):
        return f'Объединение источников (ключ основного источника: "{self.first_key}", ключ источника ' \
               f'"{self.second_source}": "{self.second_key}", время: {self.time} сек.)'


@dataclass
class StreamJoinerPlug(Operator):
    main_source: str = None

    def check(self, schema: Dict[str, SchemaFieldType], **kwargs):
        pass

    def __str__(self):
        return f'Объединение источников (основной источник: "{self.main_source}")'
