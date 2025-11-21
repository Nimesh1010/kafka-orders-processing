from kafka import KafkaConsumer, KafkaProducer
import argparse
import time
from avro_helper import load_schema, deserialize_avro

def create_consumer(broker, topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

def create_dlq_producer(broker):
    return KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: v)

running_sum = 0
count = 0

def process_message(record):
    global running_sum, count
    price = record["price"]
    running_sum += price
    count += 1
    return running_sum / count

def main(broker, topic, schema_path, max_retries):
    schema = load_schema(schema_path)
    consumer = create_consumer(broker, topic)
    dlq_producer = create_dlq_producer(broker)

    for msg in consumer:
        success = False
        retries = 0

        while not success and retries < max_retries:
            try:
                record = deserialize_avro(schema, msg.value)
                avg_price = process_message(record)
                print("Consumed:", record, " | Running Avg:", avg_price)
                success = True

            except Exception as e:
                print("Temporary Error:", e)
                retries += 1
                time.sleep(2 ** retries)  # exponential backoff

        if not success:
            print("Sending to DLQ:", msg.value)
            dlq_producer.send("orders-dlq", msg.value)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker")
    parser.add_argument("--topic")
    parser.add_argument("--schema")
    parser.add_argument("--max-retries", type=int, default=3)
    args = parser.parse_args()

    main(args.broker, args.topic, args.schema, args.max_retries)
