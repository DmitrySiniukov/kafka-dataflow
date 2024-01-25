from kafka import KafkaProducer
import json


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    print('An error has occurred.')


if __name__ == "__main__":
    target_topic = "test"
    producer = KafkaProducer(bootstrap_servers=['127.0.0.1:29092'],
                             retries=5, key_serializer=lambda x: json.dumps(x).encode("ascii"),
                             value_serializer=lambda x: json.dumps(x).encode("ascii"))
    messages = [
        {"key": "message_one", "value": "Message from PyCharm"},
        {"key": "message_two", "value": "This is Kafka-Python"}
    ]
    for msg in messages:
        producer.send(target_topic, **msg).add_callback(on_send_success).add_errback(on_send_error)

    producer.flush()
