import copy
from dataclasses import dataclass, InitVar, field
from typing import Dict, Union, Optional, List

from utils import helper
from .base import BaseEntity, SinkType, OperatorType, SourceType, SchemaFieldType
from .sinks import Sink, GreenplumSink, KafkaSink, MinioSink
from .sources import KafkaSource, Source, CloneSource
from .operators import Operator, Deduplicator, Filter, Output, Clone, FieldDeleter, FieldEnricher, FieldChanger, \
    StreamJoiner, StreamJoinerPlug

LOGGER = helper.get_logger()

OPERATOR_CONVERTER = {
    OperatorType.DEDUPLICATOR.value: Deduplicator,
    OperatorType.FILTER.value: Filter,
    OperatorType.OUTPUT.value: Output,
    OperatorType.CLONE.value: Clone,
    OperatorType.FIELD_DELETER.value: FieldDeleter,
    OperatorType.FIELD_CHANGER.value: FieldChanger,
    OperatorType.FIELD_ENRICHER.value: FieldEnricher,
    OperatorType.STREAM_JOINER.value: StreamJoiner,
    OperatorType.STREAM_JOINER_PLUG.value: StreamJoinerPlug
}


class JobConfigException(Exception):
    pass


@dataclass
class Settings(BaseEntity):
    parallelism: int = None
    min_pause_between_checkpoints: int = None
    checkpoint_interval: int = None


@dataclass
class Job(BaseEntity):
    temp: InitVar[dict] = field(default=None)
    settings: Settings = None
    sources: Dict[str, Source] = None
    sinks: Dict[str, Sink] = None
    operators: Dict[str, List[Operator]] = None

    def __post_init__(self, temp):
        if temp is not None:
            if (init_operators := temp.get('settings')) is not None:
                self.settings = Settings(**init_operators)
            if (init_operators := temp.get('sources')) is not None:
                self.sources = {name: self.create_necessary_source(source) for name, source in init_operators.items()}
            else:
                self.sources = {}
            if (init_operators := temp.get('sinks')) is not None:
                self.sinks = {name: self.create_necessary_sink(sink) for name, sink in init_operators.items()}
            else:
                self.sinks = {}
            self.operators = {}
            if (init_operators := temp.get('operators')) is not None:
                for name, operators in init_operators.items():
                    if name not in self.sources:
                        raise JobConfigException(f'Неизвестный источник "{name}"!')
                    self.fill_source(name, init_operators)
        else:
            self.sources = {}
            self.operators = {}
            self.sinks = {}

    def fill_source(self, source_name: str, operators: Dict[str, list]):
        if source_name in self.operators:
            return
        for index, operator in enumerate(source_operators := operators[source_name]):
            if operator['operator_type'] == OperatorType.CLONE.value:
                if operator['clone_name'] not in self.sources:
                    raise JobConfigException(f'Неизвестный источник "{operator["clone_name"]}"!')
                if source_name in self.operators:
                    self.operators[source_name].append(Clone(**operator))
                else:
                    self.operators[source_name] = [Clone(**operator)]
            elif operator['operator_type'] == OperatorType.STREAM_JOINER_PLUG.value:
                if index != len(source_operators) - 1:
                    raise JobConfigException(f'Некорректный оператор для второстепенного источника "{source_name}" '
                                             f'при объединении источников!')
            else:
                if operator['operator_type'] == OperatorType.STREAM_JOINER.value:
                    if (second_source := operator['second_source']) not in self.operators:
                        self.fill_source(second_source, operators)
                self.add_operator(source_name, self.create_necessary_operator(operator))

    @staticmethod
    def create_necessary_source(source: dict) -> Union[KafkaSource, CloneSource]:
        source_type = source.get('source_type')
        if source_type == SourceType.KAFKA.value:
            return KafkaSource(**source)
        elif source_type == SourceType.CLONE.value:
            return CloneSource(**source)
        else:
            raise JobConfigException('Неизвестный тип источника!')

    @staticmethod
    def create_necessary_sink(sink: dict) -> Union[GreenplumSink, KafkaSink, MinioSink]:
        sink_type = sink.get('sink_type')
        if sink_type == SinkType.GREENPLUM.value:
            return GreenplumSink(**sink)
        elif sink_type == SinkType.KAFKA.value:
            return KafkaSink(**sink)
        elif sink_type == SinkType.MINIO.value:
            return MinioSink(**sink)
        else:
            raise JobConfigException('Неизвестный тип хранилища!')

    @staticmethod
    def create_necessary_operator(operator: dict) -> Union[Deduplicator, Filter, Output, Clone, FieldDeleter,
                                                           FieldChanger, FieldEnricher, StreamJoiner, StreamJoinerPlug]:
        if (operator_type := operator.get('operator_type')) in OPERATOR_CONVERTER:
            return OPERATOR_CONVERTER[operator_type](**operator)
        raise JobConfigException('Неизвестный тип оператора!')

    def add_source(self, name: str, source: Source):
        if self.sources is None:
            self.sources = {name: source}
        elif name in self.sources:
            raise JobConfigException(f'Источник с именем "{name}" уже существует!')
        else:
            self.sources[name] = source

    def delete_cloned_sources(self, start_source_name: str):
        source_stack = [start_source_name]
        while len(source_stack):
            source_name = source_stack.pop()
            if source_name in self.operators:
                for operator in self.operators[source_name]:
                    if operator.operator_type == OperatorType.CLONE:
                        source_stack.append(getattr(operator, 'clone_name'))
                    elif operator.operator_type == OperatorType.STREAM_JOINER:
                        if len(self.operators[second_source := getattr(operator, 'second_source')]) == 1:
                            self.operators.pop(second_source)
                        else:
                            self.operators[second_source].pop()
                    elif operator.operator_type == OperatorType.STREAM_JOINER_PLUG:
                        self.delete_operator(main_source := getattr(operator, 'main_source'),
                                             next(index for index, operator in enumerate(self.operators[main_source])
                                                  if operator.operator_type == OperatorType.STREAM_JOINER and
                                                  getattr(operator, 'second_source') == source_name))
                self.operators.pop(source_name)
            self.sources.pop(source_name)

    def delete_source(self, name: str):
        if not len(self.sources):
            raise JobConfigException(f'Список источников пуст!')
        if name not in self.sources:
            raise JobConfigException(f'Источник с именем "{name}" не существует!')
        for operator in self.operators.get(name, []):
            if operator.operator_type == OperatorType.STREAM_JOINER:
                if len(self.operators[second_source := getattr(operator, 'second_source')]) == 1:
                    self.operators.pop(second_source)
                else:
                    self.operators[second_source].pop()
            elif operator.operator_type == OperatorType.STREAM_JOINER_PLUG:
                self.delete_operator(main_source := getattr(operator, 'main_source'),
                                     next(index for index, operator in enumerate(self.operators[main_source])
                                          if operator.operator_type == OperatorType.STREAM_JOINER and
                                          getattr(operator, 'second_source') == name))
        if self.sources[name].source_type == SourceType.CLONE:
            for operator in self.operators[parent := getattr(self.sources[name], 'source')]:
                if operator.operator_type == OperatorType.CLONE and getattr(operator, 'clone_name') == name:
                    self.operators[parent].remove(operator)
                    break
        self.delete_cloned_sources(name)

    def add_sink(self, name: str, sink: Sink):
        if self.sinks is None:
            self.sinks = {name: sink}
        elif name in self.sinks:
            raise JobConfigException(f'Выходное хранилище с именем "{name}" уже существует!')
        else:
            self.sinks[name] = sink

    def delete_sink(self, name: str):
        if not len(self.sinks):
            raise JobConfigException(f'Список выходных хранилищ пуст!')
        if name not in self.sinks:
            raise JobConfigException(f'Выходное хранилище с именем "{name}" не существует!')
        self.sinks.pop(name)
        for source, operators in self.operators.items():
            buffer = [operator for operator in operators if operator.operator_type != OperatorType.OUTPUT or
                      getattr(operator, "sink") != name]
            if len(buffer):
                self.operators[source] = buffer
            else:
                self.operators.pop(source)

    def get_source_schema(self, source_name: str) -> Dict[str, SchemaFieldType]:
        source_stack = []
        while self.sources[source_name].source_type == SourceType.CLONE:
            source_stack.append(source_name)
            source_name = getattr(self.sources[source_name], 'source')
        source_stack.append(source_name)
        current_schema: Dict[str, SchemaFieldType] = copy.deepcopy(getattr(self.sources[source_name], 'schema'))
        while len(source_stack):
            for operator in self.operators.get(source_stack.pop(), []):
                if operator.operator_type == OperatorType.FIELD_DELETER:
                    current_schema.pop(operator.field_name)
                elif operator.operator_type == OperatorType.FIELD_ENRICHER:
                    current_schema[operator.field_name] = SchemaFieldType.STRING
                elif operator.operator_type == OperatorType.STREAM_JOINER:
                    new_schema = self.get_source_schema(getattr(operator, 'second_source'))
                    new_schema.update(current_schema)
                    current_schema = new_schema
                elif len(source_stack) and operator.operator_type == OperatorType.CLONE and \
                        operator.clone_name == source_stack[-1]:
                    break
        return current_schema

    def add_operator(self, source: str, operator: Operator):
        if source not in self.sources:
            raise JobConfigException(f'Неизвестный источник "{source}"!')
        if len(source_operators := self.operators.get(source, [])) and \
                source_operators[-1].operator_type == OperatorType.STREAM_JOINER_PLUG:
            raise JobConfigException(f'Источник "{source}" уже влит в '
                                     f'"{getattr(source_operators[-1], "main_source")}"!')
        try:
            if operator.operator_type == OperatorType.CLONE:
                if (clone := getattr(operator, 'clone_name')) in self.sources:
                    raise JobConfigException(f'Источник с именем "{clone}" уже существует!')
                self.add_source(clone, CloneSource(source_type=SourceType.CLONE, source=source))
            elif operator.operator_type == OperatorType.OUTPUT:
                if (sink := getattr(operator, 'sink')) not in self.sinks:
                    raise JobConfigException(f'Неизвестное хранилище "{sink}"!')
                sink_entity = self.sinks[sink]
                if sink_entity.sink_type == SinkType.GREENPLUM:
                    operator.check(self.get_source_schema(source), schema_to_compare=getattr(sink_entity, 'schema'))
                elif sink_entity.sink_type == SinkType.MINIO:
                    operator.check(self.get_source_schema(source), title_name=getattr(sink_entity, 'title_name'))
            elif operator.operator_type == OperatorType.STREAM_JOINER:
                if (second_source := getattr(operator, 'second_source')) not in self.sources:
                    raise JobConfigException(f'Неизвестный источник "{second_source}"!')
                if source == second_source:
                    raise JobConfigException(f'Источники совпадают!')
                if len(source_operators := self.operators.get(second_source, [])) and \
                        source_operators[-1].operator_type == OperatorType.STREAM_JOINER_PLUG:
                    raise JobConfigException(f'Источник "{second_source}" уже влит в '
                                             f'"{getattr(source_operators[-1], "main_source")}"!')
                operator.check(self.get_source_schema(source), second_schema=self.get_source_schema(second_source))
                if second_source in self.operators:
                    self.operators[second_source].append(StreamJoinerPlug(operator_type=OperatorType.STREAM_JOINER_PLUG,
                                                                          main_source=source))
                else:
                    self.operators[second_source] = [StreamJoinerPlug(operator_type=OperatorType.STREAM_JOINER_PLUG,
                                                                      main_source=source)]
            else:
                operator.check(self.get_source_schema(source))
            if source in self.operators:
                self.operators[source].append(operator)
            else:
                self.operators[source] = [operator]
        except ValueError as error:
            raise JobConfigException(error)

    def check_operator(self, operator: Operator, schema: Dict[str, SchemaFieldType]):
        if operator.operator_type == OperatorType.OUTPUT:
            sink_entity = self.sinks[getattr(operator, 'sink')]
            if sink_entity.sink_type == SinkType.GREENPLUM:
                operator.check(schema, schema_to_compare=getattr(sink_entity, 'schema'))
            elif sink_entity.sink_type == SinkType.MINIO:
                operator.check(schema, title_name=getattr(sink_entity, 'title_name'))
        else:
            operator.check(schema)

    def check_clone(self, clone_name: str, schema: Dict[str, SchemaFieldType]):
        if clone_name in self.operators:
            for index, operator in enumerate(self.operators[clone_name]):
                if operator.operator_type == OperatorType.CLONE:
                    self.check_clone(getattr(operator, 'clone_name'), copy.deepcopy(schema))
                else:
                    try:
                        self.check_operator(operator, schema)
                    except ValueError:
                        raise JobConfigException(f'Удаление невозможно, так как в источнике "{clone_name}" есть '
                                                 f'зависимый оператор "{operator}" с индексом = {index}!')
                    if operator.operator_type == OperatorType.FIELD_DELETER:
                        schema.pop(getattr(operator, 'field_name'))
                    elif operator.operator_type == OperatorType.FIELD_ENRICHER:
                        schema[getattr(operator, 'field_name')] = SchemaFieldType.STRING
                    elif operator.operator_type == OperatorType.STREAM_JOINER:
                        new_schema = self.get_source_schema(getattr(operator, 'second_source'))
                        new_schema.update(schema)
                        schema = new_schema

    def check_operator_dependence(self, source_name: str,
                                  aim_operator: Union[FieldDeleter, FieldEnricher, StreamJoiner]):
        source_stack = []
        current_source = source_name
        while self.sources[current_source].source_type == SourceType.CLONE:
            source_stack.append(current_source)
            current_source = getattr(self.sources[current_source], 'source')
        source_stack.append(current_source)
        current_schema: Dict[str, SchemaFieldType] = copy.deepcopy(getattr(self.sources[current_source], 'schema'))
        clone_mode = False
        while len(source_stack):
            for index, operator in enumerate(self.operators.get(source_stack.pop(), [])):
                if clone_mode:
                    if operator.operator_type == OperatorType.CLONE:
                        self.check_clone(getattr(operator, 'clone_name'), copy.deepcopy(current_schema))
                    else:
                        try:
                            self.check_operator(operator, current_schema)
                        except ValueError:
                            raise JobConfigException(f'Удаление невозможно, так как в источнике "{source_name}" есть '
                                                     f'зависимый оператор "{operator}" с индексом = {index}!')
                        if operator.operator_type == OperatorType.FIELD_DELETER:
                            current_schema.pop(operator.field_name)
                        elif operator.operator_type == OperatorType.FIELD_ENRICHER:
                            current_schema[operator.field_name] = SchemaFieldType.STRING
                        elif operator.operator_type == OperatorType.STREAM_JOINER:
                            new_schema = self.get_source_schema(getattr(operator, 'second_source'))
                            new_schema.update(current_schema)
                            current_schema = new_schema
                else:
                    if not len(source_stack) and operator == aim_operator:
                        clone_mode = True
                        continue
                    if operator.operator_type == OperatorType.FIELD_DELETER:
                        current_schema.pop(operator.field_name)
                    elif operator.operator_type == OperatorType.FIELD_ENRICHER:
                        current_schema[operator.field_name] = SchemaFieldType.STRING
                    elif operator.operator_type == OperatorType.STREAM_JOINER:
                        new_schema = self.get_source_schema(getattr(operator, 'second_source'))
                        new_schema.update(current_schema)
                        current_schema = new_schema
                    elif len(source_stack) and operator.operator_type == OperatorType.CLONE and \
                            operator.clone_name == source_stack[-1]:
                        break

    def delete_operator(self, source_name: str, pos: Optional[int]):
        if source_name not in self.sources:
            raise JobConfigException(f'Источник "{source_name}" не существует!')
        if source_name not in self.operators:
            raise JobConfigException(f'Для источника "{source_name}" операторов не существует!')
        if pos is None:
            if (operator := self.operators[source_name][-1]).operator_type == OperatorType.CLONE:
                self.delete_cloned_sources(getattr(operator, 'clone_name'))
            elif operator.operator_type == OperatorType.STREAM_JOINER:
                if len(self.operators[second_source := getattr(operator, 'second_source')]) == 1:
                    self.operators.pop(second_source)
                else:
                    self.operators[second_source].pop()
            elif operator.operator_type == OperatorType.STREAM_JOINER_PLUG:
                if len(self.operators[main_source := getattr(operator, 'main_source')]) == 1:
                    self.operators.pop(main_source)
                else:
                    for operator in self.operators[main_source]:
                        if operator.operator_type == OperatorType.STREAM_JOINER and \
                                getattr(operator, 'second_source') == source_name:
                            self.check_operator_dependence(main_source, operator)
                            self.operators[main_source].remove(operator)
                            break
            if len(self.operators[source_name]) == 1:
                self.operators.pop(source_name)
            else:
                self.operators[source_name].pop()
            return
        if not 0 <= pos < (operator_count := len(self.operators[source_name])):
            raise JobConfigException(f'Невозможно удалить оператор с индексом {pos}, значение должно быть: 0 <= '
                                     f'pos <= {operator_count - 1}!')
        if (operator := self.operators[source_name][pos]).operator_type == OperatorType.CLONE:
            self.delete_cloned_sources(getattr(self.operators[source_name][pos], 'clone_name'))
        elif operator.operator_type == OperatorType.FIELD_DELETER or \
                operator.operator_type == OperatorType.FIELD_ENRICHER:
            self.check_operator_dependence(source_name, operator)
        elif operator.operator_type == OperatorType.STREAM_JOINER:
            self.check_operator_dependence(source_name, operator)
            if len(self.operators[second_source := getattr(operator, 'second_source')]) == 1:
                self.operators.pop(second_source)
            else:
                self.operators[second_source].pop()
        elif operator.operator_type == OperatorType.STREAM_JOINER_PLUG:
            if len(self.operators[main_source := getattr(operator, 'main_source')]) == 1:
                self.operators.pop(main_source)
            else:
                for operator in self.operators[main_source]:
                    if operator.operator_type == OperatorType.STREAM_JOINER and \
                            getattr(operator, 'second_source') == source_name:
                        self.check_operator_dependence(main_source, operator)
                        self.operators[main_source].remove(operator)
                        break
        if len(self.operators[source_name]) == 1:
            self.operators.pop(source_name)
        else:
            self.operators[source_name].pop(pos)

    def get_chains(self) -> str:
        return "\n\n".join(f'{self.sources[source_name].get_text_info(source_name)}: '
                           f'{" -> ".join(f"[{index}] {str(operator)}" for index, operator in enumerate(operators))}'
                           for source_name, operators in self.operators.items())
