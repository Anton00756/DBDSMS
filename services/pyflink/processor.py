import os
import time

import psycopg2
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer, JdbcSink, JdbcConnectionOptions
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import RuntimeContext, FlatMapFunction, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

import utils.helper as helper
from kafka import KafkaConsumer, errors


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


class Greenplum(ProcessFunction):
    def __init__(self, host: str, port: str, dbname: str, user: str, password: str):
        LOGGER.info('Waiting Greenplum-container...')
        for _ in range(100):
            try:
                with psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password) as conn:
                    with conn.cursor() as cur:
                        cur.execute('create table if not exists results (number bigint not null, message text)')
                LOGGER.info('Connection with Greenplum has been established')
                break
            except psycopg2.OperationalError:
                time.sleep(0.5)
        else:
            LOGGER.error('Could not connect to Greenplum')
            exit(0)
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    def process_element(self, value, context: ProcessFunction.Context):
        with psycopg2.connect(host=self.host, port=self.port, dbname=self.dbname,
                              user=self.user, password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute(f'insert into results(number, message) values({value["number"]}, \'{value["string"]}\')')
        yield value


if __name__ == '__main__':
    LOGGER = helper.get_logger()

    # time.sleep(10)

    LOGGER.info('Waiting Kafka-container...')
    for _ in range(100):
        try:
            consumer = KafkaConsumer('raw_data',
                                     bootstrap_servers=[os.environ['KAFKA_ADDRESS']])
            LOGGER.info('Connection with Kafka has been established')
            consumer.close()
            break
        except errors.NoBrokersAvailable:
            time.sleep(0.2)
            LOGGER.error('Could not connect to Kafka111')
    else:
        LOGGER.error('Could not connect to Kafka')
        exit(0)

    # LOGGER.info('Waiting Greenplum-container...')
    # for _ in range(100):
    #     try:
    #         with psycopg2.connect(host=os.environ['GP_HOST'], port=os.environ['GP_PORT'], dbname=os.environ['GP_DB'],
    #                               user=os.environ['GP_USER'], password=os.environ['GP_PASSWORD']) as conn:
    #             with conn.cursor() as cur:
    #                 cur.execute("create table if not exists results (number bigint not null, message text)")
    #                 LOGGER.info('Result table in Greenplum was created')
    #         break
    #     except psycopg2.OperationalError:
    #         time.sleep(0.5)
    # else:
    #     LOGGER.error('Could not connect to Greenplum')
    #     exit(0)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(f"file:///{os.environ.get('KAFKA_CONNECTOR_PATH', 'work_dir/flink-connector-kafka.jar')}")
    # env.add_jars(f"file:///{os.environ.get('JDBC_CONNECTOR_PATH', 'work_dir/flink-connector-jdbc.jar')}")
    # env.add_jars(f"file:///work_dir/flink-streaming-java.jar")
    # env.add_jars(f"file:///{os.environ.get('POSTGRES_CONNECTOR_PATH', 'work_dir/postgres-connector-java.jar')}")
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
    # data_stream = env.add_source(kafka_consumer)
    data_stream = env.from_collection([(i, f'test{i}') for i in range(1_000)], type_info=type_info)

    kafka_producer = FlinkKafkaProducer(
        topic='processed_data',
        serialization_schema=out_json_schema,
        producer_config={"bootstrap.servers": os.environ['KAFKA_ADDRESS']}
    )

    # greenplum_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
    #     .with_url(f"jdbc:postgresql://{os.environ['GP_HOST']}:{os.environ['GP_PORT']}/{os.environ['GP_DB']}") \
    #     .with_user_name(os.environ['GP_USER']) \
    #     .with_password(os.environ['GP_PASSWORD']) \
    #     .with_driver_name("com.postgresql.cj.jdbc.Driver") \
    #     .build()
    # greenplum_producer = JdbcSink.sink("insert into results(number, message) VALUES(?, ?)",
    #                                    type_info=Types.ROW([Types.INT(), Types.STRING()]),
    #                                    jdbc_connection_options=greenplum_options)

    # processed_data_stream = data_stream.key_by(lambda x: x.number)\
    #     .flat_map(Deduplicator(Time.minutes(10)), output_type=type_info)\
    #     .process(Greenplum(os.environ['GP_HOST'], os.environ['GP_PORT'], os.environ['GP_DB'],
    #                        os.environ['GP_USER'], os.environ['GP_PASSWORD']), output_type=type_info)
    processed_data_stream = data_stream.key_by(lambda x: x.number) \
        .flat_map(Deduplicator(Time.minutes(10)), output_type=type_info)
    processed_data_stream.add_sink(kafka_producer)
    # processed_data_stream.add_sink(greenplum_producer)
    # processed_data_stream.print()
    env.execute()
