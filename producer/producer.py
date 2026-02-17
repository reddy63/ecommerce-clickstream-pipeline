import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:29092",
            api_version_auto_timeout_ms=10000,
            request_timeout_ms=15000,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Connected to Kafka")
        break
    except NoBrokersAvailable:
        print("Waiting for Kafka...")
        time.sleep(5)

API_URL = "https://fakestoreapi.com/products"

while True:
    try:
        res = requests.get(API_URL, timeout=10)
        res.raise_for_status()

        for p in res.json():
            event = {
                "product_id": p["id"],
                "category": p["category"],
                "price": p["price"],
                "event_ts": int(time.time())
            }
            producer.send("clickstream", event)
            print("Sent:", event)
            time.sleep(1)

        time.sleep(30)

    except Exception as e:
        print("Producer error:", e)
        time.sleep(10)
