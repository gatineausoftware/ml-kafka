from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


c = AvroConsumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'groupid2',
    'schema.registry.url': 'http://127.0.0.1:8081',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})


c.subscribe(['passenger2'])

while True:
    try:
        msg = c.poll(1)

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break

    if msg is None:
        print("no message")
        continue

    if msg.error():
        print("AvroConsumer error: {}".format(msg.error()))
        continue

    print(msg.value())

c.close()
