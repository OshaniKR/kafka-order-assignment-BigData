# Kafka Order Processing System with Avro, Retry Logic & DLQ

A Kafka-based system that produces and consumes order messages using Avro serialization, with real-time aggregation, retry logic for temporary failures, and a Dead Letter Queue (DLQ) for permanently failed messages.

## Architecture

Producer → Kafka Topic (`orders`) → Consumer
- Consumer computes a running average of order prices
- Failed messages are retried up to 3 times with exponential backoff
- Messages that fail all retries are sent to a DLQ topic (`orders-dlq`)

## Tech Stack

- **Kafka** + **Zookeeper** (via Docker)
- **Confluent Schema Registry** for Avro schema management
- **Python** (`confluent-kafka` library) for producer and consumer


## Order Schema (`order.avsc`)

| Field   | Type   | Description                  |
|---------|--------|-------------------------------|
| orderId | string | Unique identifier for the order |
| product | string | Name of the purchased item   |
| price   | float  | Price of the product          |

## Setup Instructions

### 1. Start Kafka environment
```bash
docker-compose up -d
```

### 2. Create Kafka topics
```bash
docker exec -it kafka-assignment-kafka-1 kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it kafka-assignment-kafka-1 kafka-topics --create --topic orders-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the consumer (start this first)
```bash
cd consumer
python consumer.py
```

### 5. Run the producer (in a separate terminal)
```bash
cd producer
python producer.py
```

## Verifying the DLQ

To watch messages arrive in the dead letter queue:
```bash
docker exec -it kafka-assignment-kafka-1 kafka-console-consumer --topic orders-dlq --bootstrap-server localhost:9092 --from-beginning
```

## How It Works

- **Real-time aggregation**: the consumer maintains a running total and count, updating the average incrementally as each order is processed.
- **Retry logic**: `process_order()` simulates a 20% chance of a transient failure. On failure, the consumer retries up to 3 times with exponential backoff (2s, 4s, 8s).
- **Dead Letter Queue**: if an order fails all 3 retries, it is published to the `orders-dlq` topic instead of being lost, so it can be inspected or reprocessed later.
