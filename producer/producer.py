import random, time, uuid
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

with open("../schema/order.avsc") as f:
    schema_str = f.read()
sr_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
avro_serializer = AvroSerializer(sr_client, schema_str)

producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

products = ["Item1", "Item2", "Item3"]

for i in range(1000, 1050):
    order = {
        "orderId": str(i),
        "product": random.choice(products),
        "price": round(random.uniform(5.0, 500.0), 2)
    }
    producer.produce(topic="orders", key=order["orderId"], value=order, on_delivery=delivery_report)
    producer.poll(0)
    time.sleep(1)

producer.flush()