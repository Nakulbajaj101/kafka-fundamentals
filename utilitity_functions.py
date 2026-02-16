from confluent_kafka import Producer, Consumer
import textwrap
import json

class KafkaProducer:
    def __init__(self, bootsrap_server_config: dict):
        self.config = bootsrap_server_config
        self.producer = Producer(self.config)

    def acknowledgement(self, err, msg):
        if err is not None:
            print(f"Failed to deliver message: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            print(textwrap.dedent(f"""
                                  Produced event to topic {msg.topic()}:
                                    Key: {msg.key().decode('utf-8')}
                                    Value: {msg.value().decode('utf-8')}
                                    """)
            )
            
    def send_message(self, topic: str, key: str, message: dict):
        self.producer.produce(topic, 
                              json.dumps(message).encode("utf-8"), 
                              key=str(message[key]),
                              callback=self.acknowledgement)
        self.producer.poll(0)


class KafkaConsumer:
    def __init__(self, bootsrap_server_config: dict, consumer_name: str):
        self.config = bootsrap_server_config
        self.consumer = Consumer(self.config)
        self.consumer_name = consumer_name

    def subscribe(self, topics: list[str]):
        self.consumer.subscribe(topics)

    def consume_messages(self, process_message_callback: callable):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"{self.consumer_name}: Consumer error: {msg.error()}")
                    continue
                process_message_callback(msg, self.consumer_name)
        finally:
            self.consumer.close()

