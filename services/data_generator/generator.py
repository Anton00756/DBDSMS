import json
import os
import random
import time

from kafka import KafkaProducer, errors

from redis import Redis
from utils import helper

PERSON_COUNT = 1000
DUPLICATE_FACTOR = 2


def person_numbers(count: int):
    for i in range(count):
        yield str(i + 1000000), f'Person №{i}'


def generate_calls(count: int):
    for j in range(DUPLICATE_FACTOR):
        for i in range(count):
            yield {'number': i + 1000000, 'call_time': random.randint(0, 300)}


if __name__ == '__main__':
    LOGGER = helper.get_logger()

    LOGGER.info('Waiting Redis-container...')
    with Redis(os.environ['REDIS_HOST'], int(os.environ['REDIS_PORT'])) as redis_conn:
        for key, value in person_numbers(PERSON_COUNT):
            redis_conn.set(key, value)
    LOGGER.info('Data was generated and sent to Redis')

    LOGGER.info('Waiting Kafka-container...')
    for _ in range(100):
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_ADDRESS'],
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            for value in generate_calls(PERSON_COUNT):
                producer.send('calls', value)
            producer.flush()
            producer.close()
            LOGGER.info('Data was generated and sent to Kafka')
            exit(0)
        except errors.NoBrokersAvailable:
            time.sleep(0.2)
    else:
        LOGGER.error('Could not connect to Kafka')
