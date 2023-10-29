import enum
from abc import ABC
from typing import Optional, List, Dict, Union
from utils import helper
from dataclasses import dataclass, InitVar, field

LOGGER = helper.get_logger()


class SinkType(enum.Enum):
    GREENPLUM = 'greenplum'
    KAFKA = 'kafka'
    MINIO = 'minio'


class JobConfigException(Exception):
    pass


class BaseEntity(ABC):
    def to_json(self) -> dict:
        result = {}
        for key, value in vars(self).items():
            if hasattr(value, 'to_json'):
                result[key] = value.to_json()
            elif isinstance(value, list):
                result[key] = [item.to_json() if hasattr(item, 'to_json') else item for item in value]
            elif isinstance(value, dict):
                result[key] = {key: item.to_json() if hasattr(item, 'to_json') else item for key, item in value.items()}
            elif isinstance(value, SinkType):
                result[key] = value.value
            else:
                result[key] = value
        return result


@dataclass
class Settings(BaseEntity):
    parallelism: int = None
    min_pause_between_checkpoints: int = None
    checkpoint_interval: int = None


@dataclass
class Source(BaseEntity):
    address: str = None
    topic: str = None
    schema: Dict[str, str] = None


@dataclass
class Sink(BaseEntity):
    sink_type: SinkType = None
    address: str = None


@dataclass
class KafkaSink(Sink):
    topic: str = None


@dataclass
class GreenplumSink(Sink):
    table: str = None
    schema: Dict[str, str] = None
    init_sql: str = None


@dataclass
class MinioSink(Sink):
    title_field: str = None


@dataclass
class Job(BaseEntity):
    temp: InitVar[dict] = field(default=None)
    settings: Settings = None
    sources: Dict[str, Source] = None
    sinks: Dict[str, Sink] = None
    operators: dict = None

    def __post_init__(self, temp):
        if temp is not None:
            if (value := temp.get('settings')) is not None:
                self.settings = Settings(**value)
            if (value := temp.get('sources')) is not None:
                self.sources = {name: Source(**source) for name, source in value.items()}
            else:
                self.sources = {}
            if (value := temp.get('sinks')) is not None:
                self.sinks = {name: self.create_necessary_sink(sink) for name, sink in value.items()}
            else:
                self.sinks = {}

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
            raise JobConfigException('Неизвестный тип хранилища!')

    def add_source(self, name: str, source: Source):
        if self.sources is None:
            self.sources = {name: source}
        elif name in self.sources:
            raise JobConfigException(f'Источник с именем "{name}" уже существует!')
        else:
            self.sources[name] = source

    def delete_source(self, name: str):
        if not len(self.sources):
            raise JobConfigException(f'Список источников пуст!')
        if name not in self.sources:
            raise JobConfigException(f'Источник с именем "{name}" не существует!')
        self.sources.pop(name)

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
