import json
import os
import time

from kafka import KafkaConsumer, errors

import utils.helper as helper


if __name__ == '__main__':
    LOGGER = helper.get_logger()
    LOGGER.info('Waiting Kafka-container...')
    for _ in range(100):
        try:
            consumer = KafkaConsumer('processed_data',
                                     bootstrap_servers=[os.environ['KAFKA_ADDRESS']],
                                     auto_offset_reset='earliest',
                                     value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            for message in consumer:
                LOGGER.info(f'Message: {message}')
        except errors.NoBrokersAvailable:
            time.sleep(0.1)
    else:
        LOGGER.error('Could not connect to Kafka')

