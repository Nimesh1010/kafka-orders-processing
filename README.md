Kafka Orders Processing System

A simple Kafka-based order processing pipeline built using Python, Avro, and Kafka.
This project includes a Producer, Consumer, Avro serialization, error handling with Dead Letter Queue (DLQ), and utilities to test message flow.

Project Structure

kafka-orders/
├── schemas/
│   └── order.avsc
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
├── avro_helper.py
├── quick_dlq_reader.py
├── requirements.txt
└── README.md

Description

schemas/order.avsc → Avro schema for order messages
producer/producer.py → Produces order messages to Kafka
consumer/consumer.py → Consumes and processes messages
avro_helper.py → Helper for Avro encode/decode
quick_dlq_reader.py → Reads messages from Dead Letter Queue
requirements.txt → Python dependencies


Features

Kafka Producer & Consumer
Avro Serialization & Deserialization
Retry Logic for failed messages
Dead Letter Queue (DLQ) Support
Running Average Calculation
Schema-based message validation
Clean project structure for assignments