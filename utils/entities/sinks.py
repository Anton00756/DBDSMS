from dataclasses import dataclass
from typing import Dict

from .base import BaseEntity, SinkType


@dataclass
class Sink(BaseEntity):
    sink_type: SinkType = None
    address: str = None

    def __post_init__(self):
        self.sink_type = SinkType(self.sink_type)


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
