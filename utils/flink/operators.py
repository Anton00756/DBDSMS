from typing import List, Set

from pyflink.common import Time
from pyflink.common.typeinfo import Types, TypeInformation, RowTypeInfo
from pyflink.common.types import Row
from pyflink.datastream.functions import RuntimeContext, FlatMapFunction, MapFunction, CoFlatMapFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from redis import Redis

import utils.helper as helper

LOGGER = helper.get_logger()


class Deduplicator(FlatMapFunction):
    def __init__(self, time_to_live: Time):
        self.ttl = time_to_live
        self.key_was_seen = None
        self.counter_in = None
        self.counter_out = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("key_was_seen", Types.BOOLEAN())
        state_ttl_config = StateTtlConfig \
            .new_builder(self.ttl) \
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite) \
            .disable_cleanup_in_background() \
            .build()
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.key_was_seen = runtime_context.get_state(state_descriptor)
        self.counter_in = runtime_context.get_metrics_group().counter("deduplicator_in")
        self.counter_out = runtime_context.get_metrics_group().counter("deduplicator_out")

    def flat_map(self, value):
        self.counter_in.inc()
        if self.key_was_seen.value() is None:
            self.key_was_seen.update(True)
            self.counter_out.inc()
            yield value


class FieldUpdater(MapFunction):
    def __init__(self, field_name: str, field_value: str, field_type):
        self.field_name = field_name
        self.field_value = field_value
        self.field_type = field_type

    def map(self, value):
        value[self.field_name] = self.field_type(eval(self.field_value, {'item': value}))
        return value


class FieldDeleter(MapFunction):
    def __init__(self, fields: List[str]):
        self.fields = fields
        self.row_generator = Row(*fields)

    def map(self, value):
        return self.row_generator(*[value[key] for key in self.fields])


class FieldEnricher(MapFunction):
    def __init__(self, host: str, port: int, field_name: str, search_key: str, fields: List[str]):
        self.host = host
        self.port = port
        self.conn = None
        self.field_name = field_name
        self.search_key = search_key
        self.fields = fields
        self.row_generator = Row(*fields)

    def open(self, runtime_context: RuntimeContext):
        self.conn = Redis(self.host, self.port)

    def map(self, value):
        new_field = str(eval(self.search_key, {'item': value})) if (field := self.conn.get(
            str(eval(self.search_key, {'item': value})))) is None \
            else field.decode('utf-8')
        return self.row_generator(*[value[key] if key != self.field_name else new_field for key in self.fields])

    def close(self):
        self.conn.close()


class StreamJoiner(CoFlatMapFunction):
    def __init__(self, time_to_live: Time, first_template: tuple, second_template: tuple, result_fields: List[str],
                 main_fields: Set[str]):
        self.ttl = time_to_live
        self.first_template = Types.ROW_NAMED(*first_template)
        self.second_template = Types.ROW_NAMED(*second_template)
        self.first_stream_value = None
        self.second_stream_value = None
        self.result_fields = result_fields
        self.row_generator = Row(*result_fields)
        self.main_fields = main_fields

    def open(self, runtime_context: RuntimeContext):
        state_ttl_config = StateTtlConfig \
            .new_builder(self.ttl) \
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite) \
            .disable_cleanup_in_background() \
            .build()

        first_state_descriptor = ValueStateDescriptor("first_stream", self.first_template)
        first_state_descriptor.enable_time_to_live(state_ttl_config)
        self.first_stream_value = runtime_context.get_state(first_state_descriptor)
        second_state_descriptor = ValueStateDescriptor("second_stream", self.second_template)
        second_state_descriptor.enable_time_to_live(state_ttl_config)
        self.second_stream_value = runtime_context.get_state(second_state_descriptor)

    def flat_map1(self, value):
        if self.second_stream_value.value() is None:
            self.first_stream_value.update(value)
        else:
            buffer_value = self.second_stream_value.value()
            self.second_stream_value.clear()
            yield self.row_generator(*[value[key] if key in self.main_fields else buffer_value[key]
                                       for key in self.result_fields])

    def flat_map2(self, value):
        if self.first_stream_value.value() is None:
            self.second_stream_value.update(value)
        else:
            buffer_value = self.first_stream_value.value()
            self.first_stream_value.clear()
            yield self.row_generator(*[value[key] if key not in self.main_fields else buffer_value[key]
                                       for key in self.result_fields])
