import json
import os

from kafka import KafkaConsumer


if __name__ == '__main__':
    consumer = KafkaConsumer('raw_data',
                             bootstrap_servers=[os.environ['KAFKA_ADDRESS']],
                             auto_offset_reset='earliest',
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    for message in consumer:
        print(message)
