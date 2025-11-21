from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders-dlq",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Reading DLQ Messages...")

for msg in consumer:
    print("DLQ Message:", msg.value)
