import copy
import os
import time
from typing import Dict

import psycopg2
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer, JdbcSink, JdbcConnectionOptions, \
    FileSink
from pyflink.datastream.connectors.file_system import RollingPolicy, OutputFileConfig
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema

from utils import helper
from utils.config_manager import ConfigManager
from utils.entities import Job, SourceType, SchemaFieldType, SinkType, OperatorType
from utils.flink import JsonEncoder, KeyBucketAssigner, Deduplicator, FieldEnricher, StreamJoiner, FieldUpdater, \
    FieldDeleter, Filter
from utils.grafana_builder import GrafanaBuilder

LOGGER = helper.get_logger()


class JobExecutor:
    FIELD_TYPE_TO_FLINK = {
        SchemaFieldType.BOOL: Types.BOOLEAN, SchemaFieldType.INT: Types.INT, SchemaFieldType.DOUBLE: Types.DOUBLE,
        SchemaFieldType.STRING: Types.STRING
    }

    def __init__(self, job: Job):
        self.job = job
        self.environment = StreamExecutionEnvironment.get_execution_environment()
        self.compiled_sources = set()
        self.clones = {}
        self.streams_to_join = {}
        self.grafana_builder = GrafanaBuilder()

    def prepare_flink_env(self):
        [self.environment.add_jars(f"file:///{jar}") for jar in os.environ['JAR_FILES'].split(';')]
        self.environment.enable_checkpointing(30000, CheckpointingMode.EXACTLY_ONCE)
        self.environment.get_checkpoint_config().enable_unaligned_checkpoints()
        self.environment.get_checkpoint_config().set_max_concurrent_checkpoints(1)
        self.environment.get_checkpoint_config().set_checkpoint_timeout(60000)
        settings = self.job.settings
        if settings is None:
            self.environment.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
            self.environment.get_checkpoint_config().set_checkpoint_interval(30000)
        else:
            if settings.parallelism is not None:
                self.environment.set_parallelism(settings.parallelism)
            self.environment.get_checkpoint_config().set_min_pause_between_checkpoints(
                5000 if settings.min_pause_between_checkpoints is None else settings.min_pause_between_checkpoints
            )
            self.environment.get_checkpoint_config().set_checkpoint_interval(
                30000 if settings.checkpoint_interval is None else settings.checkpoint_interval
            )

    @staticmethod
    def get_lists_for_named_row_by_schema(schema: Dict[str, SchemaFieldType]):
        names = []
        types = []
        for field_name in sorted(schema.keys()):
            names.append(field_name)
            types.append(JobExecutor.FIELD_TYPE_TO_FLINK[schema[field_name]]())
        return names, types

    @staticmethod
    def get_named_row_by_schema(schema: Dict[str, SchemaFieldType]):
        return Types.ROW_NAMED(*JobExecutor.get_lists_for_named_row_by_schema(schema))

    def add_source(self, name: str):
        source = self.job.sources[name]
        kafka_consumer = FlinkKafkaConsumer(
            getattr(source, 'topic'),
            deserialization_schema=JsonRowDeserializationSchema.builder().type_info(
                self.get_named_row_by_schema(getattr(source, 'schema'))
            ).build(),
            properties={"bootstrap.servers": os.environ['KAFKA_ADDRESS'], "group.id": f"source_{name}"}
        )
        kafka_consumer.set_start_from_earliest()
        return self.environment.add_source(kafka_consumer)

    def add_sink(self, datastream: DataStream, name: str, schema: Dict[str, SchemaFieldType]):
        sink = self.job.sinks[name]
        if sink.sink_type == SinkType.KAFKA:
            kafka_sink = FlinkKafkaProducer(
                topic=getattr(sink, 'topic'), serialization_schema=JsonRowSerializationSchema.builder().with_type_info(
                    self.get_named_row_by_schema(schema)).build(),
                producer_config={"bootstrap.servers": os.environ['KAFKA_ADDRESS']}
            )
            datastream.add_sink(kafka_sink)
        elif sink.sink_type == SinkType.MINIO:
            minio_sink = FileSink.for_row_format(base_path=f"s3://{sink.address}/", encoder=JsonEncoder()) \
                .with_bucket_assigner(KeyBucketAssigner(getattr(sink, 'title_field'))) \
                .with_output_file_config(OutputFileConfig.builder().with_part_suffix('.json').build()) \
                .with_rolling_policy(RollingPolicy.default_rolling_policy(1)).build()
            datastream.sink_to(minio_sink)
        else:
            greenplum_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
                .with_url(f"jdbc:postgresql://{sink.address}/{getattr(sink, 'database')}") \
                .with_user_name(os.environ['GP_USER']) \
                .with_password(os.environ['GP_PASSWORD']) \
                .with_driver_name("org.postgresql.Driver") \
                .build()
            sink_schema = getattr(sink, "schema")
            greenplum_sink = JdbcSink.sink(
                f'insert into {getattr(sink, "table")}'
                f'({", ".join(sink_schema[field] for field in sorted(sink_schema.keys()))}) '
                f'VALUES({", ".join(["?"] * len(sink_schema))})',
                type_info=self.get_named_row_by_schema(schema),
                jdbc_connection_options=greenplum_options
            )
            datastream.add_sink(greenplum_sink)

    def process_datastream(self, source_name: str):
        if source_name in self.compiled_sources:
            return
        if (source := self.job.sources[source_name]).source_type == SourceType.CLONE:
            if source_name not in self.clones:
                self.process_datastream(getattr(source, 'source'))
            datastream, schema = self.clones[source_name]
        else:
            datastream = self.add_source(source_name)
            schema = getattr(source, 'schema')
        for index, operator in enumerate(self.job.operators[source_name]):
            if operator.operator_type == OperatorType.DEDUPLICATOR:
                self.add_sink(datastream, 'kafka_sink', schema)
                datastream = datastream.key_by(lambda item: item[getattr(operator, 'key')]) \
                    .process(Deduplicator(Time.seconds(getattr(operator, 'time')), f'{source_name}:{index}'),
                             output_type=self.get_named_row_by_schema(schema))
                self.grafana_builder.add_deduplicator(source_name, index)
            elif operator.operator_type == OperatorType.FILTER:
                datastream = datastream.flat_map(Filter(getattr(operator, 'expression'), f'{source_name}:{index}'))
                self.grafana_builder.add_filter(source_name, index)
            elif operator.operator_type == OperatorType.CLONE:
                self.clones[getattr(operator, 'clone_name')] = (datastream, copy.deepcopy(schema))
            elif operator.operator_type == OperatorType.FIELD_DELETER:
                schema.pop(getattr(operator, 'field_name'))
                datastream = datastream.map(FieldDeleter(sorted(schema.keys())),
                                            output_type=self.get_named_row_by_schema(schema))
            elif operator.operator_type == OperatorType.FIELD_ENRICHER:
                field_name = getattr(operator, 'field_name')
                schema[field_name] = SchemaFieldType.STRING
                datastream = datastream.map(FieldEnricher(
                    os.environ['REDIS_HOST'], int(os.environ['REDIS_PORT']), field_name,
                    getattr(operator, 'search_key'), sorted(schema.keys()), f'{source_name}:{index}'),
                    output_type=self.get_named_row_by_schema(schema))
                self.grafana_builder.add_enricher(source_name, index)
            elif operator.operator_type == OperatorType.FIELD_CHANGER:
                field_name = getattr(operator, 'field_name')
                if (new_type := getattr(operator, 'new_type')) is not None:
                    schema[field_name] = new_type
                datastream = datastream.map(FieldUpdater(field_name, getattr(operator, 'value'), SchemaFieldType.
                                                         get_python_type_by_field_type(schema[field_name])),
                                            output_type=self.get_named_row_by_schema(schema))
            elif operator.operator_type == OperatorType.STREAM_JOINER:
                if (second_source := getattr(operator, 'second_source')) not in self.streams_to_join:
                    self.process_datastream(second_source)
                second_datastream, second_schema = self.streams_to_join[second_source]
                first_template = self.get_lists_for_named_row_by_schema(schema)
                second_template = self.get_lists_for_named_row_by_schema(second_schema)
                main_source_keys = schema.keys()
                second_schema.update(schema)
                schema = second_schema
                datastream = datastream.connect(second_datastream). \
                    key_by(lambda x: x[getattr(operator, 'first_key')], lambda y: y[getattr(operator, 'second_key')]) \
                    .process(StreamJoiner(Time.seconds(getattr(operator, 'time')), first_template, second_template,
                                          sorted(schema.keys()), main_source_keys, f'{source_name}:{index}'),
                             output_type=self.get_named_row_by_schema(schema))
                self.grafana_builder.add_stream_joiner(source_name, index, second_source)
            elif operator.operator_type == OperatorType.STREAM_JOINER_PLUG:
                self.streams_to_join[source_name] = (datastream, copy.deepcopy(schema))
            else:
                self.add_sink(datastream, getattr(operator, 'sink'), schema)
        self.compiled_sources.add(source_name)

    def run(self):
        self.prepare_flink_env()
        for name, sink in self.job.sinks.items():
            if sink.sink_type == SinkType.GREENPLUM:
                for retry in range(1, 101):
                    try:
                        host, port = sink.address.split(':')
                        with psycopg2.connect(host=host, port=port, dbname=getattr(sink, 'database'),
                                              user=os.environ['GP_USER'], password=os.environ['GP_PASSWORD']) as conn:
                            with conn.cursor() as cur:
                                cur.execute(getattr(sink, 'init_sql'))
                        break
                    except psycopg2.OperationalError:
                        LOGGER.error(f'[{retry}] Could not init Greenplum sink with name "{name}"')
                        time.sleep(1)
        for source_name in self.job.operators.keys():
            self.process_datastream(source_name)
        self.grafana_builder.save('shared_data/grafana.json')
        self.environment.execute()


if __name__ == '__main__':
    executor = JobExecutor(ConfigManager(os.environ['CONFIG_FOR_JOB_PATH']).job)
    executor.run()
