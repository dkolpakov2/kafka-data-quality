import os
import json
import time
import random
from kafka import KafkaProducer


BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "topic_onprem_sales")
TOPIC2 = os.getenv("KAFKA_TOPIC2", "topic_cloud_sales")
TOPIC3 = os.getenv("KAFKA_TOPIC3", "topic1")
TOPIC4 = os.getenv("KAFKA_TOPIC4", "topic2")
RATE = float(os.getenv("MESSAGE_RATE", "1"))
LIMIT = int(os.getenv("MESSAGE_LIMIT", "4"))   # Number of messages to send 

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
        "event_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "value": round(random.uniform(-100, 500), 2),
        "source": "seeder"
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


def main():
    wait_for_kafka()

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Starting seeding → topic '{TOPIC}' at rate {RATE} msg/sec")

    counter = 0
    sleep_time = 1.0 / RATE

    LIMIT = 2  # Example limit
    for counter in range(LIMIT):
        msg = generate_message(counter)
        producer.send(TOPIC, msg)
        msg = generate_message2(counter)
        producer.send(TOPIC2, msg)
        msg = generate_message3(counter)
        producer.send(TOPIC3, msg)
        print(f"Sent {counter+1}/{LIMIT}:", msg)
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
