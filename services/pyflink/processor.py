import os

from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import RuntimeContext, FlatMapFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

import utils.helper as helper


class Deduplicator(FlatMapFunction):
    def __init__(self, time_to_live: Time):
        self.ttl = time_to_live
        self.key_was_seen = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("key_was_seen", Types.BOOLEAN())
        state_ttl_config = StateTtlConfig \
            .new_builder(self.ttl) \
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite) \
            .disable_cleanup_in_background() \
            .build()
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.key_was_seen = runtime_context.get_state(state_descriptor)

    def flat_map(self, value):
        if self.key_was_seen.value() is None:
            self.key_was_seen.update(True)
            yield value


if __name__ == '__main__':
    LOGGER = helper.get_logger()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(f"file:///{os.environ.get('KAFKA_CONNECTOR_PATH', 'work_dir/flink-connector-kafka.jar')}")
    env.enable_checkpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(120000)
    env.get_checkpoint_config().enable_unaligned_checkpoints()
    env.get_checkpoint_config().set_checkpoint_interval(30000)

    type_info = Types.ROW_NAMED(['number', 'string'],
                                [Types.INT(), Types.STRING()])
    in_json_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()
    out_json_schema = JsonRowSerializationSchema.builder().with_type_info(type_info).build()

    kafka_consumer = FlinkKafkaConsumer(
        'raw_data',
        deserialization_schema=in_json_schema,
        properties={"bootstrap.servers": os.environ['KAFKA_ADDRESS']}
    )

    kafka_consumer.set_start_from_earliest()
    data_stream = env.add_source(kafka_consumer)
    kafka_producer = FlinkKafkaProducer(
        topic='processed_data',
        serialization_schema=out_json_schema,
        producer_config={"bootstrap.servers": os.environ['KAFKA_ADDRESS']}
    )

    # kafka_producer2 = FlinkKafkaProducer(
    #     topic='processed_data2',
    #     serialization_schema=out_json_schema,
    #     producer_config={"bootstrap.servers": os.environ['KAFKA_ADDRESS']}
    # )

    # data_stream.key_by(lambda x: x.number).flat_map(Deduplicator(Time.minutes(1)),
    #                                                 output_type=type_info).add_sink(kafka_producer)
    processed_data_stream = data_stream.key_by(lambda x: x.number).flat_map(Deduplicator(Time.minutes(10)),
                                                                            output_type=type_info)
    processed_data_stream.add_sink(kafka_producer)
    # processed_data_stream.add_sink(kafka_producer2)
    env.execute()
