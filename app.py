import os
import json
from kafka import KafkaConsumer, KafkaProducer
from prediction import predict

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BROKER_LIST') or 'modh-demo-kafka-bootstrap:9092'
CONSUMER_GROUP = 'object-detection-consumer-group'
CONSUMER_TOPIC = os.getenv('KAFKA_TOPIC_IMAGES') or 'images'
PRODUCER_TOPIC = os.getenv('KAFKA_TOPIC_OBJECTS') or 'objects'


def main():
    consumer = KafkaConsumer(CONSUMER_TOPIC,
                             group_id=CONSUMER_GROUP,
                             auto_offset_reset='earliest',
                             bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             api_version_auto_timeout_ms=30000,
                             request_timeout_ms=450000)

    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                             api_version_auto_timeout_ms=30000,
                             max_block_ms=900000,
                             request_timeout_ms=450000,
                             acks='all')

    print(f'Subscribed to "{KAFKA_BOOTSTRAP_SERVER}" consuming topic "{CONSUMER_TOPIC}, producing messages on topic "{PRODUCER_TOPIC}"...')

    try:
        for record in consumer:
            msg = record.value.decode('utf-8')
            dict = json.loads(msg)
            result = predict(dict)
            dict['prediction'] = result
            producer.send(PRODUCER_TOPIC, json.dumps(dict).encode('utf-8'))
            producer.flush()
    finally:
        print("Closing KafkaTransformer...")
        consumer.close()
    print("Kafka transformer stopped.")


if __name__ == '__main__':
    main()
