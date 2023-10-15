import json
import os
import time

from kafka import KafkaProducer, errors

from redis import Redis
from utils import helper

DATA_TO_SEND = [
    {'number': 1, 'string': 'test'},
    {'number': 2, 'string': 'test2'},
    {'number': 1, 'string': 'test3'}
]


def generate_data():
    # i = 1
    # return {'number': i, 'string': f'test{i}'}
    for i in range(10_000):
        yield {'number': i, 'string': f'test{i}'}


def generate_reversed_data():
    # i = 1
    # return {'number': i, 'reversed_string': f'{i}tset'}
    # for i in range(1_000, 2_000):
    #     yield {'number': i, 'reversed_string': f'{i}tset'}
    for i in range(1_000):
        yield {'number': i, 'reversed_string': f'{i}tset'}


if __name__ == '__main__':
    LOGGER = helper.get_logger()

    LOGGER.info('Waiting Redis-container...')
    with Redis(os.environ['REDIS_HOST'], int(os.environ['REDIS_PORT'])) as redis_conn:
        for i in range(1_000):
            redis_conn.set(f'test{i}', f'{i}test')
    LOGGER.info('Data was generated and sent to Redis')

    LOGGER.info('Waiting Kafka-container...')
    for _ in range(100):
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_ADDRESS'],
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            [producer.send('raw_data', value) for value in generate_data()]
            [producer.send('second_raw_data', value) for value in generate_reversed_data()]
            producer.flush()
            producer.close()
            LOGGER.info('Data was generated and sent to Kafka')
            exit(0)
        except errors.NoBrokersAvailable:
            time.sleep(0.2)
    else:
        LOGGER.error('Could not connect to Kafka')
