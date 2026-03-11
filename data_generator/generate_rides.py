import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Producer started. Sending ride_requests and driver_locations...")

while True:

    # Use real current time so Spark windowing works correctly
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # --- Topic 1: ride_requests ---
    ride_event = {
        "ride_id":     random.randint(1000, 9999),
        "user_id":     random.randint(1, 100),
        "pickup_lat":  round(random.uniform(21.1, 21.2), 6),
        "pickup_long": round(random.uniform(79.0, 79.1), 6),
        "timestamp":   now
    }
    producer.send("ride_requests", ride_event)
    print("[ride_requests]    Sent:", ride_event)

    # --- Topic 2: driver_locations ---
    driver_event = {
        "driver_id":   random.randint(200, 300),
        "driver_lat":  round(random.uniform(21.1, 21.2), 6),
        "driver_long": round(random.uniform(79.0, 79.1), 6),
        "status":      random.choice(["available", "on_trip", "offline"]),
        "timestamp":   now
    }
    producer.send("driver_locations", driver_event)
    print("[driver_locations] Sent:", driver_event)

    time.sleep(2)