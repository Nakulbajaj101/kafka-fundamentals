from utilitity_functions import KafkaConsumer
import json
import argparse

def process_message(msg, consumer_name):
    event = json.loads(msg.value().decode("utf-8"))
    data = json.loads(event)
    bot = data.get("bot", False) # Assuming bot edits are less important for our use case, we can skip them
    minor = data.get("minor", True) # Assuming minor edits are less important for our use case, we can skip them as well
    if bot and not minor:
        return
    

    print(f"""Consumer {consumer_name} Processed edit: {data['title']} from customer {data['id']} 
            within partiton : {msg.partition()} from user {data['user']} with comment {data['comment']}""")

def main():
    parser = argparse.ArgumentParser(description="Kafka Consumer")
    parser.add_argument("--group-id", "-g",type=str, default="consumer-group", help="Consumer group ID", required=True)
    parser.add_argument("--topic", "-t", type=str, default="orders", help="Kafka topic to consume from", required=True)
    parser.add_argument("--name", "-n", type=str, default="consumer", help="Name of the consumer instance", required=True)
    args = parser.parse_args()

    group_id = args.group_id
    topic = args.topic
    consumer_name = args.name

    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    }

    consumer = KafkaConsumer(config, consumer_name)
    consumer.subscribe(topics=[topic])
    consumer.consume_messages(process_message_callback=process_message)


if __name__ == "__main__":
    main()
