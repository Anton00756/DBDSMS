import json
import os

from kafka import KafkaProducer


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_ADDRESS'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('raw_data', {'number': 1})
    producer.flush()
