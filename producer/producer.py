import time
import random
import uuid
from kafka import KafkaProducer
import argparse
from avro_helper import load_schema, serialize_avro

PRODUCTS = ["Laptop", "Phone", "Tablet", "Headphones", "Camera"]

def create_producer(broker):
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: v
    )

def generate_order():
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(10, 500), 2)
    }

def main(broker, topic, schema_path, interval, count):
    schema = load_schema(schema_path)
    producer = create_producer(broker)

    for _ in range(count):
        order = generate_order()
        data = serialize_avro(schema, order)
        producer.send(topic, data)
        print("Produced:", order)
        time.sleep(interval)

    producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker")
    parser.add_argument("--topic")
    parser.add_argument("--schema")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--count", type=int, default=20)
    args = parser.parse_args()

    main(args.broker, args.topic, args.schema, args.interval, args.count)
