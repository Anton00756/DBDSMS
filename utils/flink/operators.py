from typing import List, Set

from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.common.types import Row
from pyflink.datastream.functions import RuntimeContext, FlatMapFunction, MapFunction, CoFlatMapFunction, \
    KeyedProcessFunction, CoProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from redis import Redis

import utils.helper as helper

LOGGER = helper.get_logger()


class Deduplicator(KeyedProcessFunction):
    def __init__(self, time_to_live: Time, operator_name: str):
        self.ttl = time_to_live
        self.key_time = None
        self.counter_in = None
        self.counter_out = None
        self.operator_name = operator_name

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("key_time", Types.LONG())
        state_ttl_config = StateTtlConfig \
            .new_builder(self.ttl) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .never_return_expired() \
            .cleanup_full_snapshot() \
            .build()
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.key_time = runtime_context.get_state(state_descriptor)
        self.counter_in = runtime_context.get_metrics_group().counter(f"[{self.operator_name}] deduplicator_in")
        self.counter_out = runtime_context.get_metrics_group().counter(f"[{self.operator_name}] deduplicator_out")

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        self.counter_in.inc()
        if (time := self.key_time.value()) is None or time < ctx.timestamp():
            self.key_time.update(ctx.timestamp() + self.ttl.to_milliseconds())
            self.counter_out.inc()
            yield value


class Filter(FlatMapFunction):
    def __init__(self, expression: str, operator_name: str):
        self.expression = expression
        self.counter_in = None
        self.counter_out = None
        self.operator_name = operator_name

    def open(self, runtime_context: RuntimeContext):
        self.counter_in = runtime_context.get_metrics_group().counter(f"[{self.operator_name}] filter_in")
        self.counter_out = runtime_context.get_metrics_group().counter(f"[{self.operator_name}] filter_out")

    def flat_map(self, value):
        self.counter_in.inc()
        if eval(self.expression, {'item': value}):
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
    def __init__(self, host: str, port: int, field_name: str, search_key: str, fields: List[str], operator_name: str):
        self.host = host
        self.port = port
        self.conn = None
        self.field_name = field_name
        self.search_key = search_key
        self.fields = fields
        self.row_generator = Row(*fields)
        self.counter_in = None
        self.enrichment_counter = None
        self.operator_name = operator_name

    def open(self, runtime_context: RuntimeContext):
        self.conn = Redis(self.host, self.port)
        self.counter_in = runtime_context.get_metrics_group().counter(f"[{self.operator_name}] enricher_in")
        self.enrichment_counter = runtime_context.get_metrics_group().\
            counter(f"[{self.operator_name}] enrichment_count")

    def map(self, value):
        self.counter_in.inc()
        if (field := self.conn.get(str(eval(self.search_key, {'item': value})))) is None:
            new_field = ''
        else:
            self.enrichment_counter.inc()
            new_field = field.decode('utf-8')
        return self.row_generator(*[value[key] if key != self.field_name else new_field for key in self.fields])

    def close(self):
        self.conn.close()


class StreamJoiner(CoProcessFunction):
    def __init__(self, time_to_live: Time, first_template: tuple, second_template: tuple, result_fields: List[str],
                 main_fields: Set[str], operator_name: str):
        self.ttl = time_to_live
        self.first_template = Types.ROW_NAMED(*first_template)
        self.second_template = Types.ROW_NAMED(*second_template)
        self.first_key_time = None
        self.second_key_time = None
        self.first_stream_value = None
        self.second_stream_value = None
        self.result_fields = result_fields
        self.row_generator = Row(*result_fields)
        self.main_fields = main_fields
        self.counter_first_in = None
        self.counter_second_in = None
        self.counter_out = None
        self.operator_name = operator_name

    def open(self, runtime_context: RuntimeContext):
        state_ttl_config = StateTtlConfig \
            .new_builder(self.ttl) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .never_return_expired() \
            .cleanup_full_snapshot() \
            .build()

        first_key_time_descriptor = ValueStateDescriptor("first_key_time", Types.LONG())
        first_key_time_descriptor.enable_time_to_live(state_ttl_config)
        self.first_key_time = runtime_context.get_state(first_key_time_descriptor)
        first_state_descriptor = ValueStateDescriptor("first_stream", self.first_template)
        first_state_descriptor.enable_time_to_live(state_ttl_config)
        self.first_stream_value = runtime_context.get_state(first_state_descriptor)
        second_key_time_descriptor = ValueStateDescriptor("second_key_time", Types.LONG())
        second_key_time_descriptor.enable_time_to_live(state_ttl_config)
        self.second_key_time = runtime_context.get_state(second_key_time_descriptor)
        second_state_descriptor = ValueStateDescriptor("second_stream", self.second_template)
        second_state_descriptor.enable_time_to_live(state_ttl_config)
        self.second_stream_value = runtime_context.get_state(second_state_descriptor)

        self.counter_first_in = runtime_context.get_metrics_group()\
            .counter(f"[{self.operator_name}] stream_joiner_first_in")
        self.counter_second_in = runtime_context.get_metrics_group()\
            .counter(f"[{self.operator_name}] stream_joiner_second_in")
        self.counter_out = runtime_context.get_metrics_group().counter(f"[{self.operator_name}] stream_joiner_out")

    def process_element1(self, value, ctx: 'CoProcessFunction.Context'):
        self.counter_first_in.inc()
        if self.second_stream_value.value() is None or self.second_key_time.value() < ctx.timestamp():
            self.first_stream_value.update(value)
            self.first_key_time.update(ctx.timestamp() + self.ttl.to_milliseconds())
        else:
            buffer_value = self.second_stream_value.value()
            self.second_stream_value.clear()
            self.counter_out.inc()
            yield self.row_generator(*[value[key] if key in self.main_fields else buffer_value[key]
                                       for key in self.result_fields])

    def process_element2(self, value, ctx: 'CoProcessFunction.Context'):
        self.counter_second_in.inc()
        if self.first_stream_value.value() is None or self.first_key_time.value() < ctx.timestamp():
            self.second_stream_value.update(value)
            self.second_key_time.update(ctx.timestamp() + self.ttl.to_milliseconds())
        else:
            buffer_value = self.first_stream_value.value()
            self.first_stream_value.clear()
            self.counter_out.inc()
            yield self.row_generator(*[value[key] if key not in self.main_fields else buffer_value[key]
                                       for key in self.result_fields])
