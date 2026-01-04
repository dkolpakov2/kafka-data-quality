import os
import json
import time
import random
import hashlib
from kafka import KafkaProducer


BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "topic_onprem")
TOPIC2 = os.getenv("KAFKA_TOPIC2", "topic_cloud")
TOPIC3 = os.getenv("KAFKA_TOPIC3", "topic1")
TOPIC4 = os.getenv("KAFKA_TOPIC4", "topic2")
RATE = float(os.getenv("MESSAGE_RATE", 1))
LIMIT = int(os.getenv("MESSAGE_LIMIT", 4))   # Number of messages to send 

def wait_for_kafka():
    print("Waiting for Kafka broker:", BROKER)
    while True:
        try:
            p = KafkaProducer(bootstrap_servers=BROKER)
            p.close()
            print("Kafka is ready.")
            return
        except Exception as e:
            print("Kafka not ready:", e)
            time.sleep(3)


def generate_message(counter):
    return {
        "id": str(counter),
        "value": round(random.uniform(-100, 500), 2),
        "source": "seeder",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

def generate_message2(counter):
    return {
        "pk": str(counter),
        "hash": round(random.uniform(-100, 500), 2),
        "payload": "seeder",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def generate_message3(counter):
    return {
        "pk": str(counter),
        "hash": round(random.uniform(-100, 500), 2),
        "payload": "seeder",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def generate_message4(counter):
    return {
        "pk": str(counter),
        "hash": round(random.uniform(-100, 500), 2),
        "payload": "seeder",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def generate_message_with_hash(counter, payload):
    payload_hash = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    return {
        "pk": str(counter),
        "hash": payload_hash,
        "payload": payload,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

def main():
    wait_for_kafka()

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Starting sending messages to → topic '{TOPIC}' at rate {RATE} msg/sec")

    counter = 0
    sleep_time = 1.0 / RATE

    for counter in range(LIMIT):
        payload = f"message_payload_{counter}"
        msg = generate_message_with_hash(counter, payload)
        producer.send(TOPIC, msg)
        print(f"Sent {TOPIC} {counter+1}/{LIMIT}:", msg)
        producer.send(TOPIC2, msg)
        print(f"Sent {TOPIC2} {counter+1}/{LIMIT}:", msg)
        msg = generate_message_with_hash(counter, payload)
        producer.send(TOPIC3, msg)
        print(f"Sent {TOPIC3} {counter+1}/{LIMIT}:", msg)
        producer.send(TOPIC4, msg)
        print(f"Sent {TOPIC4} {counter+1}/{LIMIT}:", msg)
        time.sleep(sleep_time)


    producer.flush()
    print(f"Completed seeding {LIMIT} messages. Exiting.")
    # while True:
    #     msg = generate_message(counter)
    #     producer.send(TOPIC, msg)
    #     print("Sent:", msg)
    #     counter += 1
    #     time.sleep(sleep_time)


if __name__ == "__main__":
    main()
