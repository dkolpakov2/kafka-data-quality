# Feed Kafka Topic by python script in Zeppelin

##  Create Zeppelin Paragraph:

## It will do:
| Feature                | Status            |
| ---------------------- | ----------------- |
| Kafka producer         | ✅                 |
| JSON messages          | ✅                 |
| Rate limit             | ✅ 10 messages/min |
| Zeppelin compatible    | ✅                 |
| Works with SQL Gateway | ✅                 |
## MEssage Format:
{
  "pk": "uuid",
  "hash": "hash_uuid",
  "payload": "payload_1",
  "ts": "2026-01-04T10:15:00.000Z"
}

## Will fit in:
## SQL:
CREATE TABLE topic1_source (
  pk STRING,
  hash STRING,
  payload STRING,
  ts TIMESTAMP(3)
)

## How to verify messages
# flink.sql
SELCT * FROM topic1_source


## CLI:
docker exec -it kafka \
  kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic topic1 \
  --from-beginning

## Confluent Cloud
BOOTSTRAP_SERVERS = "pkc-xxx.aws.confluent.cloud:9092"
SASL_MECHANISM = "PLAIN"
SASL_USERNAME = "API KEY"
SASL_PASSWORD = "API SECRET"

## 🔹 AWS MSK (IAM via SCRAM)
SASL_MECHANISM = "SCRAM-SHA-512"

## Azure Event Hubs (Kafka)
SASL_MECHANISM = "PLAIN"
SASL_USERNAME = "$ConnectionString"
SASL_PASSWORD = "Endpoint=sb://..."

## Required Files if TLS

Required File Mounts (Docker)
Your Zeppelin container must have access to keystore & truststore:
# Structure:
kafka-secrets/
├── client.keystore.jks
└── client.truststore.jks


zeppelin:
  image: apache/zeppelin:0.11.2
  volumes:
    - ./kafka-secrets:/opt/kafka/secrets

# Structure:
volumes:
  - ./certs:/opt/certs

## Kafka Broker Requirements
  listeners=SSL://:9093
ssl.client.auth=required

## Validate SSL Connectivity inside Zeppelin:
keytool -list -keystore /opt/kafka/secrets/client.keystore.jks


## Notes / Pitfalls

✔ Kafka-Python supports JKS directly
✔ No SASL used
✔ Mutual TLS authentication
✔ Works with Kafka Connect & Flink SQL

## Fails for 
import from kafka KafkaProducer

>> add to new paragraph:
import sys
!pip install kafka-python
print(sys.version)
print(sys.path)

>> or in Docker:
RUN pip install kafka-python
>> or Dockerfile:
	Dockerfile (CORRECT)
	FROM apache/zeppelin:0.11.2

	USER root

	# Upgrade pip first (CRITICAL)
	RUN python3 -m pip install --upgrade pip setuptools wheel

	# Install compatible kafka-python
	RUN pip install kafka-python==2.0.2

	USER zeppelin

>> Build:
docker-compose build --no-cache zeppelin
docker-compose up -d

>> and validate:
docker exec -it zeppelin python -c "import kafka; print(kafka.__version__)"

## Test Connectivity
  docker exec -it zeppelin \
  python - <<EOF
	from kafka import KafkaProducer
	print("Kafka client OK")
	EOF


## %flink.pyflink

from kafka import KafkaProducer
import json
import time
import uuid
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "topic1"
MESSAGES_PER_MINUTE = 10
INTERVAL_SECONDS = 60 / MESSAGES_PER_MINUTE
TOTAL_MESSAGES = 10   # total messages to send (change if needed)

# -----------------------------
# PRODUCER
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

print(f"Producing {TOTAL_MESSAGES} messages to topic '{TOPIC}' "
      f"at {MESSAGES_PER_MINUTE} msg/min")

# -----------------------------
# SEND LOOP
# -----------------------------
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
    print(f"[{i+1}/{TOTAL_MESSAGES}] Sent message with pk={pk}")

    time.sleep(INTERVAL_SECONDS)

producer.close()
print("Done sending messages.")


##  ======================#############################################################
