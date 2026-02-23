from utilitity_functions import KafkaProducer
from requests_sse import EventSource
import json

class WikipediaProducer:
    def __init__(self, kafka_bootstrap_config: dict, kafka_topic: str, url: str):
        self.producer = KafkaProducer(bootsrap_server_config=kafka_bootstrap_config)
        self.kafka_topic = kafka_topic
        self.url = url
        self.headers = { # Replace with your actual info as per Wikimedia policy
            "User-Agent": "Bond/1.0 (https://github.com/Nakulbajaj101; bajaj.nakul@gmail.com)"
        }

    def start_producing(self):
        with EventSource(self.url, headers=self.headers) as stream:
            for event in stream:
                print(event)
                if event.type == "message" and event.data:
                    try:
                        message_data = json.loads(event.data)
                    except json.JSONDecodeError as e:
                        print(f"Failed to decode JSON: {e}")
                        continue
                    id = message_data.get("id")
                    message = {
                        "id": id,
                        "type": message_data.get("type"),
                        "user": message_data.get("user"),
                        "bot": message_data.get("bot"),
                        "title": message_data.get("title"),
                        "timestamp": message_data.get("timestamp"),
                        "comment": message_data.get("comment"),
                        "minor": message_data.get("minor", False),
                    }

                    value = json.dumps(message)
                    self.producer.send_message(self.kafka_topic, str(id), value)


def main():
    kafka_bootstrap_config = {
        "bootstrap.servers": "localhost:9092"
    }
    kafka_topic = "wikipedia-recent-changes"
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    wiki_producer = WikipediaProducer(kafka_bootstrap_config, kafka_topic, url)
    wiki_producer.start_producing()
    wiki_producer.producer.producer.flush()

if __name__ == "__main__":
    main()
