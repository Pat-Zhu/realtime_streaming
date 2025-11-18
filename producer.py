import json
import time
import uuid
import random
from datetime import datetime

from kafka import KafkaProducer


# 创建 Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_trip_event():
    """
    生成一条模拟网约车行程数据
    """

    # 以纽约附近为例，随便选一个大概的位置范围
    start_lat_base = 40.7128   # NYC
    start_lng_base = -74.0060
    end_lat_base = 40.7306
    end_lng_base = -73.9352

    distance_km = round(random.uniform(1, 15), 2)
    price_usd = round(3 + distance_km * random.uniform(1.2, 2.5), 2)

    event = {
        "trip_id": str(uuid.uuid4()),
        "driver_id": f"D-{random.randint(1000, 9999)}",
        "passenger_id": f"P-{random.randint(1000, 9999)}",
        "start_lat": start_lat_base + random.uniform(-0.02, 0.02),
        "start_lng": start_lng_base + random.uniform(-0.02, 0.02),
        "end_lat": end_lat_base + random.uniform(-0.02, 0.02),
        "end_lng": end_lng_base + random.uniform(-0.02, 0.02),
        "distance_km": distance_km,
        "price_usd": price_usd,
        "ts": datetime.now().isoformat(),
    }
    return event


if __name__ == "__main__":
    print("🚕 Starting ride-sharing producer. Press Ctrl+C to stop.")
    try:
        while True:
            event = generate_trip_event()
            # 发送到 Kafka 的 trips topic
            producer.send("trips", value=event)
            producer.flush()
            print(f"Sent trip {event['trip_id']} | ${event['price_usd']}")
            time.sleep(1)  # 每秒生成一条
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

