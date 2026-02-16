import random
import time
from datetime import datetime

from utilitity_functions import KafkaProducer

def generate_order():
    countries = ["USA", "Canada", "UK", "Germany", "France"]
    order = {
        "order_id": random.randint(1, 1000),
        "customer_id": random.randint(1, 100),
        "total_price": round(random.uniform(20.0, 1000.0), 2),
        "customer_country": random.choice(countries),
        "merchant_country": random.choice(countries),
        "order_date": datetime.now().isoformat()
    }
    return order


def main():
    config = {
        "bootstrap.servers": "localhost:9092"
    }
    topic = "orders"
    kafka_producer = KafkaProducer(config)

    for _ in range(1000):
        order = generate_order()
        kafka_producer.send_message(topic, "order_id", order)
        time.sleep(0.1)

if __name__ == "__main__":
    main()
