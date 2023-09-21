import json
import os
import time

from kafka import KafkaProducer, errors

import utils.helper as helper

DATA_TO_SEND = [
    {'number': 1, 'string': 'test'},
    {'number': 2, 'string': 'test2'},
    {'number': 1, 'string': 'test3'}
]


def generate_data():
    for i in range(1_000):
        yield {'number': i, 'string': f'test{i}'}
    # yield from DATA_TO_SEND


if __name__ == '__main__':
    LOGGER = helper.get_logger()
    LOGGER.info('Waiting Kafka-container...')
    for _ in range(100):
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_ADDRESS'],
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            [producer.send('raw_data', value) for value in generate_data()]
            producer.flush()
            producer.close()
            LOGGER.info('Data was generated and sent to Kafka')
            exit(0)
        except errors.NoBrokersAvailable:
            time.sleep(0.2)
    else:
        LOGGER.error('Could not connect to Kafka')
