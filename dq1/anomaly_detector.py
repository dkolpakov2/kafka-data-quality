## simpler service (good for testing), add dq/anomaly_detector.py
# rolling z-score using exponential moving average
import os, json, math
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS","kafka:9092")
TOPIC_VALID = os.getenv("VALID_TOPIC","validated_input")
TOPIC_ANOMALY = os.getenv("ANOMALY_TOPIC","anomalies")

consumer = KafkaConsumer(TOPIC_VALID, bootstrap_servers=BOOTSTRAP,
                         value_deserializer=lambda v: json.loads(v.decode()), group_id="anomaly-det")
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode())

alpha = 0.1
state = {}  # key -> {mean,var, count}

threshold = float(os.getenv("Z_THRESHOLD","3.0"))

for rec in consumer:
    msg = rec.value
    key = msg.get("id","__global")
    val = float(msg.get("value",0.0))
    s = state.get(key, {"mean":val,"m2":0.0,"count":1})
    if s["count"]==1:
        s["mean"] = val
        s["m2"] = 0.0
        s["count"]=1
    else:
        s["count"] += 1
        delta = val - s["mean"]
        s["mean"] += alpha * delta
        s["m2"] = (1-alpha) * s["m2"] + alpha * delta * delta
    state[key]=s
    std = math.sqrt(s["m2"] + 1e-9)
    z = (val - s["mean"]) / (std if std>0 else 1.0)
    if abs(z) > threshold:
        anomaly = {"id": key, "value": val, "z": z, "ts": msg.get("timestamp")}
        producer.send(TOPIC_ANOMALY, anomaly)
