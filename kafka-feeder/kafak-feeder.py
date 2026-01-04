%flink.pyflink

from kafka import KafkaProducer
import json
import time
import uuid
from datetime import datetime

# ==========================
# KAFKA SECURITY CONFIG
# ==========================
BOOTSTRAP_SERVERS = "broker1:9093,broker2:9093"
TOPIC = "topic1"

SECURITY_PROTOCOL = "SASL_SSL"
SASL_MECHANISM = "PLAIN"  # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512

SASL_USERNAME = "YOUR_USERNAME"
SASL_PASSWORD = "YOUR_PASSWORD"

CA_FILE = "/opt/certs/ca.pem"  # optional

# ==========================
# RATE LIMIT
# ==========================
MESSAGES_PER_MINUTE = 10
INTERVAL_SECONDS = 60 / MESSAGES_PER_MINUTE
TOTAL_MESSAGES = 10

# ==========================
# PRODUCER
# ==========================
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
    security_protocol=SECURITY_PROTOCOL,
    sasl_mechanism=SASL_MECHANISM,
    sasl_plain_username=SASL_USERNAME,
    sasl_plain_password=SASL_PASSWORD,
    ssl_cafile=CA_FILE,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    linger_ms=10,
    retries=5,
    acks="all"
)

print(f"Producing {TOTAL_MESSAGES} messages to {TOPIC} "
      f"({MESSAGES_PER_MINUTE} msg/min, SASL_SSL)")

# ==========================
# SEND LOOP
# ==========================
for i in range(TOTAL_MESSAGES):
    pk = str(uuid.uuid4())

    message = {
        "pk": pk,
        "hash": f"hash_{pk}",
        "payload": f"payload_{i}",
        "ts": datetime.utcnow().isoformat()
    }

    producer.send(
        topic=TOPIC,
        key=pk,
        value=message
    )

    producer.flush()
    print(f"[{i+1}/{TOTAL_MESSAGES}] Sent pk={pk}")

    time.sleep(INTERVAL_SECONDS)

producer.close()
print("Kafka publish complete.")
