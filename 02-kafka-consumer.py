from utilitity_functions import KafkaConsumer
import json
import argparse


def process_message(msg, consumer_name):
    print(f"Received message: {msg.value().decode('utf-8')}")
    order = json.loads(msg.value().decode("utf-8"))
    order_price = order.get("total_price", 0)
    if order_price < 500:
        return 
    
    print(f"""Consumer {consumer_name} Processed order: {order['order_id']} from customer {order['customer_id']} 
            within partiton : {msg.partition()} with total price {order_price}""")

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
