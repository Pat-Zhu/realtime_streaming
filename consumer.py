import json
import psycopg2
from kafka import KafkaConsumer


# 1. 连接 PostgreSQL
conn = psycopg2.connect(
    dbname="kafka_db",
    user="kafka_user",
    password="kafka_pass",
    host="localhost",
    port=5432,
)
cur = conn.cursor()

# 2. 创建 Kafka consumer
consumer = KafkaConsumer(
    "trips",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    # 如果是第一次跑，会从最早的消息开始消费
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="trips-consumer-group-1",
)

print("📥 Starting consumer, listening to 'trips' topic...")


# 3. 循环消费消息并写入数据库
for msg in consumer:
    data = msg.value

    try:
        cur.execute(
            """
            INSERT INTO trips (
                trip_id, driver_id, passenger_id,
                start_lat, start_lng,
                end_lat, end_lng,
                distance_km, price_usd, ts
            ) VALUES (
                %(trip_id)s, %(driver_id)s, %(passenger_id)s,
                %(start_lat)s, %(start_lng)s,
                %(end_lat)s, %(end_lng)s,
                %(distance_km)s, %(price_usd)s, %(ts)s
            )
            """,
            data,
        )
        conn.commit()
        print(f"Inserted trip {data['trip_id']} into Postgres.")
    except Exception as e:
        conn.rollback()
        print("Failed to insert record:", e)

