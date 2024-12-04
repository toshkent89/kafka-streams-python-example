import json
import time
import random
import string
import sys
import argparse
from confluent_kafka import Producer, Consumer, KafkaException

keys = ['AA', 'BB', 'CC']
colors = ['green', 'red', 'yellow']


# Main config
def create_common_config():
    return {
        'bootstrap.servers': 'kafka.broker:9093',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'user',
        'sasl.password': 'password',
        'ssl.ca.location': 'ca.crt',
    }


# Create producer
def create_producer():
    config = create_common_config()
    config['acks'] = 'all'
    return Producer(config)


# Create consumer
def create_consumer(topic):
    config = create_common_config()
    config['group.id'] = f'consumer-group-{random.randint(1, 100000)}'
    config['auto.offset.reset'] = 'earliest'
    consumer = Consumer(config)
    consumer.subscribe([topic])
    return consumer


# Generate a random message
def generate_random_message():
    key = random.choice(keys)
    N = random.randint(1, 1000)
    color = random.choice(colors)
    message = f"{color}-{N}"

    return key, message


# Callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")


# Producer
def run_producer(topic, message_count):
    producer = create_producer()

    if message_count is None:
        try:
            while True:
                key, message = generate_random_message()
                data = {
                    'key': key,
                    'value': message
                }
                json_message = json.dumps(data).encode('utf-8')
                print(f"Sending JSON message: {json_message}")
                producer.produce(topic, key=key.encode('utf-8'), value=json_message, callback=delivery_report)
                producer.poll(1)
                time.sleep(1)

        except KeyboardInterrupt:
            print("Producer stopped.")
        finally:
            producer.flush()

    else:
        for _ in range(message_count):
            key, message = generate_random_message()
            data = {
                    'key': key,
                    'value': message
            }
            json_message = json.dumps(data).encode('utf-8')
            print(f"Sending JSON message: {json_message}")
            producer.produce(topic, key=key.encode('utf-8'), value=json_message, callback=delivery_report)
            producer.poll(1)
            time.sleep(1)

        print("Finished sending messages.")
        producer.flush()


# Consumer
def run_consumer(topic):
    consumer = create_consumer(topic)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            print(f"Consumed message: {msg.key().decode('utf-8')} {msg.value().decode('utf-8')}")

            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


# Main
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer/Consumer")
    parser.add_argument('mode', choices=['producer', 'consumer'], help="Run as either producer or consumer")
    parser.add_argument('topic', help="The Kafka topic to use")
    parser.add_argument('--count', type=int, help="Number of messages to send when running as producer")

    args = parser.parse_args()

    if args.mode == 'producer':
        run_producer(args.topic, args.count)
    elif args.mode == 'consumer':
        run_consumer(args.topic)