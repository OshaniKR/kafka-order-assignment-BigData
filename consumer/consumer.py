import json, time
from confluent_kafka import DeserializingConsumer, SerializingProducer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

with open("../schema/order.avsc") as f:
    schema_str = f.read()
sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_deserializer = AvroDeserializer(sr_client, schema_str)
avro_serializer = AvroSerializer(sr_client, schema_str)

consumer = DeserializingConsumer({
    'bootstrap.servers': 'localhost:9092',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
})

# DLQ producer
dlq_producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
})

consumer.subscribe(['orders'])

running_total = 0.0
count = 0
MAX_RETRIES = 3

def process_order(order):
    """Simulate processing that can randomly fail (temporary error)."""
    import random
    if random.random() < 1.0:
        raise ConnectionError("Simulated transient downstream failure")
    return True

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        order = msg.value()
        if order is None:
            continue

        attempt = 0
        success = False
        while attempt < MAX_RETRIES and not success:
            try:
                process_order(order)
                success = True
            except ConnectionError as e:
                attempt += 1
                wait = 2 ** attempt 
                print(f"Retry {attempt}/{MAX_RETRIES} for order {order['orderId']} after error: {e}. Waiting {wait}s")
                time.sleep(wait)

        if success:
            running_total += order['price']
            count += 1
            avg = running_total / count
            print(f"Order {order['orderId']} processed. Running avg price: {avg:.2f}")
        else:
            print(f"Order {order['orderId']} failed permanently. Sending to DLQ.")
            dlq_producer.produce(topic="orders-dlq", key=order['orderId'], value=order)
            dlq_producer.poll(0)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    dlq_producer.flush()
