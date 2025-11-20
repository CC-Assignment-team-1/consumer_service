import os
import sys
import uuid
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
import boto3

# ---------- Kafka CONFIG ----------
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
api_key = os.getenv('KAFKA_API_KEY')
api_secret = os.getenv('KAFKA_API_SECRET')

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': api_key,
    'sasl.password': api_secret,
    'group.id': 'random-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

# ---------- DynamoDB CONFIG ----------
dynamodb = boto3.resource(
    'dynamodb',
    region_name=os.getenv("AWS_REGION", "ap-south-1"),  # Change if needed
)

TABLE_NAME = os.getenv("DDB_TABLE_NAME", "RandomNumbers")
table = dynamodb.Table(TABLE_NAME)

def save_to_dynamodb(number):
    """Insert a record into DynamoDB"""
    item = {
        "id": str(uuid.uuid4()),
        "random_number": int(number),
        "timestamp": datetime.utcnow().isoformat()
    }
    table.put_item(Item=item)
    print(f"Saved to DynamoDB: {item}")


def consume_numbers():
    """Consume messages from Kafka and write to DynamoDB"""
    consumer.subscribe(['topic1'])
    print("Listening for messages on 'topic1'...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            value = msg.value().decode("utf-8")
            print(f"Consumed from Kafka: {value}")

            save_to_dynamodb(value)

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    consume_numbers()
