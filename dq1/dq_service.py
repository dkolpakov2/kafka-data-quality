import os
import json
from kafka import KafkaProducer, KafkaConsumer

TOPIC_IN = os.getenv("INPUT_TOPIC")
TOPIC_VALID = os.getenv("VALID_TOPIC")
TOPIC_INVALID = os.getenv("INVALID_TOPIC")

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

consumer = KafkaConsumer(
    TOPIC_IN,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="dq-validator"
)

# Example schema requirement
REQUIRED_FIELDS = ["id", "timestamp", "value"]

def validate_message(msg):
    if not all(k in msg for k in REQUIRED_FIELDS):
        return False
    if not isinstance(msg["value"], (int, float)):
        return False
    return True

print("Python DQ Service Started...")

for message in consumer:
    msg = message.value

    if validate_message(msg):
        producer.send(TOPIC_VALID, msg)
        print("VALID:", msg)
    else:
        producer.send(TOPIC_INVALID, msg)
        print("INVALID:", msg)

