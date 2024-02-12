from abc import abstractmethod
from dataclasses import dataclass
from typing import Dict

from .base import BaseEntity, SchemaFieldType, SourceType


@dataclass
class Source(BaseEntity):
    source_type: SourceType = None

    def __post_init__(self):
        self.source_type = SourceType(self.source_type)

    @abstractmethod
    def get_text_info(self, source_name: str) -> str:
        pass


@dataclass
class KafkaSource(Source):
    address: str = None
    topic: str = None
    schema: Dict[str, SchemaFieldType] = None

    def __post_init__(self):
        super().__post_init__()
        self.schema = {key: SchemaFieldType(value) for key, value in self.schema.items()}

    def get_text_info(self, source_name: str) -> str:
        return f'Источник "{source_name}" (адрес: "{self.address}", топик: "{self.topic}")'


@dataclass
class CloneSource(Source):
    source: str = None

    def __post_init__(self):
        super().__post_init__()

    def get_text_info(self, source_name: str) -> str:
        return f'Источник "{source_name}" (копия источника "{self.source}")'
