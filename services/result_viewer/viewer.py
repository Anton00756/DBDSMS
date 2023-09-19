import json
import os

from kafka import KafkaConsumer

import utils.helper as helper


if __name__ == '__main__':
    LOGGER = helper.get_logger()

    consumer = KafkaConsumer('processed_data',
                             bootstrap_servers=[os.environ['KAFKA_ADDRESS']],
                             auto_offset_reset='earliest',
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    for message in consumer:
        LOGGER.info(f'Message: {message}')
