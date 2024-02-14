import json
import os
import time

from kafka import KafkaConsumer, errors

from utils import helper


if __name__ == '__main__':
    LOGGER = helper.get_logger()
    LOGGER.info('Waiting Kafka-container...')
    for _ in range(100):
        try:
            consumer = KafkaConsumer('person_calls',
                                     bootstrap_servers=[os.environ['KAFKA_ADDRESS']],
                                     auto_offset_reset='earliest',
                                     value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            LOGGER.info('Connection with Kafka has been established')
            for message in consumer:
                LOGGER.info(f'Message: {message}')
        except errors.NoBrokersAvailable:
            time.sleep(0.2)
    else:
        LOGGER.error('Could not connect to Kafka')

