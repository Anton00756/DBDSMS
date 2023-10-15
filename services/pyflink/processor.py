import os

from pyflink.common import Time, WatermarkStrategy, Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer, JdbcSink, JdbcConnectionOptions, \
    FileSink, KafkaSource
from pyflink.datastream.connectors.file_system import RollingPolicy, OutputFileConfig
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

from utils import helper
from utils.flink import JsonEncoder, KeyBucketAssigner, Deduplicator, UpdateEnrichment, AddEnrichment, StreamJoiner

LOGGER = helper.get_logger()


def get_prepared_flink_env():
    environment = StreamExecutionEnvironment.get_execution_environment()
    [environment.add_jars(f"file:///{jar}") for jar in os.environ['JAR_FILES'].split(';')]
    environment.enable_checkpointing(30000, CheckpointingMode.EXACTLY_ONCE)
    environment.get_checkpoint_config().set_min_pause_between_checkpoints(60000)
    environment.get_checkpoint_config().enable_unaligned_checkpoints()
    environment.get_checkpoint_config().set_checkpoint_interval(30000)
    return environment


if __name__ == '__main__':
    env = get_prepared_flink_env()

    type_info = Types.ROW_NAMED(['number', 'string'],
                                [Types.INT(), Types.STRING()])
    out_type_info = Types.ROW_NAMED(['number', 'string', 'string2'],
                                    [Types.INT(), Types.STRING(), Types.STRING()])
    in_json_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()
    out_json_schema = JsonRowSerializationSchema.builder().with_type_info(out_type_info).build()

    kafka_consumer = FlinkKafkaConsumer(
        'raw_data',
        deserialization_schema=in_json_schema,
        properties={"bootstrap.servers": os.environ['KAFKA_ADDRESS'], "group.id": "pyflink"}
    )
    kafka_consumer.set_start_from_earliest()
    data_stream = env.add_source(kafka_consumer)

    second_type_info = Types.ROW_NAMED(['number', 'reversed_string'],
                                       [Types.INT(), Types.STRING()])
    second_in_json_schema = JsonRowDeserializationSchema.builder().type_info(second_type_info).build()
    second_kafka_consumer = FlinkKafkaConsumer(
        'second_raw_data',
        deserialization_schema=second_in_json_schema,
        properties={"bootstrap.servers": os.environ['KAFKA_ADDRESS'], "group.id": "pyflink2"}
    )
    second_kafka_consumer.set_start_from_earliest()
    second_data_stream = env.add_source(second_kafka_consumer)

    kafka_producer = FlinkKafkaProducer(
        topic='processed_data',
        serialization_schema=out_json_schema,
        producer_config={"bootstrap.servers": os.environ['KAFKA_ADDRESS']}
    )

    greenplum_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url(f"jdbc:postgresql://{os.environ['GP_HOST']}:{os.environ['GP_PORT']}/{os.environ['GP_DB']}") \
        .with_user_name(os.environ['GP_USER']) \
        .with_password(os.environ['GP_PASSWORD']) \
        .with_driver_name("org.postgresql.Driver") \
        .build()
    greenplum_producer = JdbcSink.sink("insert into results(number, message, add_message) VALUES(?, ?, ?)",
                                       type_info=out_type_info, jdbc_connection_options=greenplum_options)

    minio_producer = FileSink.for_row_format(base_path=f"s3://{os.environ['RESULT_BUCKET']}/", encoder=JsonEncoder()) \
        .with_bucket_assigner(KeyBucketAssigner('number')) \
        .with_output_file_config(OutputFileConfig.builder().with_part_suffix('.json').build()) \
        .with_rolling_policy(RollingPolicy.default_rolling_policy(1)).build()

    data_stream.connect(second_data_stream).key_by(lambda x: x['number'], lambda x: x['number'])\
        .flat_map(StreamJoiner(Time.minutes(5), (('number', 'string'),
                                                 (Types.INT(), Types.STRING())), (('number', 'reversed_string'),
                                                                                  (Types.INT(), Types.STRING()))),
                  output_type=out_type_info).print()
    # processed_data_stream = first.connect(second).key_by(lambda x: x['number'], lambda x: x['number'])\
    #     .process(StreamJoiner(Time.minutes(1), type_info, type_info), output_type=out_type_info)
    # processed_data_stream.print()
    # processed_data_stream = data_stream.key_by(lambda x: x['number'])\
    #     .connect(second_data_stream.key_by(lambda x: x['number'])).process(StreamJoiner(Time.minutes(1),
    #                                                                                     type_info, second_type_info),
    #                                                                        output_type=out_type_info)
    # processed_data_stream.add_sink(kafka_producer)

    # processed_data_stream = data_stream.key_by(lambda x: x['number']) \
    #     .flat_map(Deduplicator(Time.minutes(10)), output_type=type_info) \
    #     .map(UpdateEnrichment(os.environ['REDIS_HOST'], int(os.environ['REDIS_PORT'])), output_type=type_info) \
    #     .map(AddEnrichment(os.environ['REDIS_HOST'], int(os.environ['REDIS_PORT'])), output_type=out_type_info)
    # processed_data_stream.add_sink(kafka_producer)
    # processed_data_stream.add_sink(greenplum_producer)
    # processed_data_stream.sink_to(minio_producer)
    env.execute()
