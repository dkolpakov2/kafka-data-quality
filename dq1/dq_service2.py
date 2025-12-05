import os, json, time
from kafka import KafkaProducer, KafkaConsumer
from schema_validator import validate as schema_validate
from rule_engine import RuleEngine

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_IN = os.getenv("INPUT_TOPIC", "raw_input")
TOPIC_VALID = os.getenv("VALID_TOPIC", "validated_input")
TOPIC_INVALID = os.getenv("INVALID_TOPIC", "dq_failures")
TOPIC_QUARANTINE = os.getenv("QUARANTINE_TOPIC", "dq_quarantine")

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode())
consumer = KafkaConsumer(TOPIC_IN, bootstrap_servers=BOOTSTRAP, auto_offset_reset="earliest",
                         value_deserializer=lambda v: json.loads(v.decode()), group_id="dq-validator")

engine = RuleEngine("rules.yaml")
SUBJECT = os.getenv("SCHEMA_SUBJECT", "raw_input-value")  # subject in confluent schema registry

print("DQ service started - schema mode:", os.getenv("SCHEMA_MODE","confluent"))

for rec in consumer:
    msg = rec.value
    # 1) Schema registry check
    ok, reason = schema_validate(msg, SUBJECT)
    if not ok:
        msg["_dq_reasons"] = [{"id":"schema", "severity":"fail", "message":reason}]
        producer.send(TOPIC_INVALID, msg)
        continue

    # 2) Rule engine
    is_valid, failures = engine.apply(msg)
    if is_valid:
        producer.send(TOPIC_VALID, msg)
    else:
        # route based on highest severity
        if any(f["severity"]=="fail" for f in failures):
            msg["_dq_reasons"] = failures
            producer.send(TOPIC_INVALID, msg)
        else:
            msg["_dq_reasons"] = failures
            producer.send(TOPIC_QUARANTINE, msg)
