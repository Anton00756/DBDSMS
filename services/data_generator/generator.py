import json
import os
import time

import kafka.errors
from kafka import KafkaProducer

import utils.helper as helper


if __name__ == '__main__':
    LOGGER = helper.get_logger()

    LOGGER.info('Waiting KAFKA-container...')
    kafka_state = False
    while not kafka_state:
        try:
            producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_ADDRESS'],
                                     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            producer.send('raw_data', {'number': 1})
            producer.flush()
            kafka_state = True
        except kafka.errors.NoBrokersAvailable:
            time.sleep(0.1)
    LOGGER.info('Data was generated and sent to Kafka')
