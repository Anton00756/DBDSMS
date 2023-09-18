"""
Написать Flink-приложение с чтением Kafka,
дедупликацией с интервалом в 10 минут,
и отправкой сообщений и в Kafka и в GreenPlum.
"""
from kafka import KafkaConsumer
from pyflink import datastream
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from pyflink.datastream.formats.json import JsonRowDeserializationSchema


row_type_info = Types.ROW_NAMED(['id', 'name', 'subject', 'content'],
                                [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()])
json_schema = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()

kafka_properties = {
    "bootstrap.servers": 'kafka:9092',
    # "sasl.mechanism": "SCRAM-SHA-256",
    # "security.protocol": "SASL_SSL",
    # "sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{user_name}" password="{user_pass}";'
}

env = StreamExecutionEnvironment.get_execution_environment()
source = FlinkKafkaConsumer(
    topic,
    deserialization_schema=json_schema,
    properties=kafka_properties
)


