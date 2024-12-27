import json
from kafka import KafkaConsumer
from opensearchpy import OpenSearch
import logging

KAFKA_BROKER = ["52.6.122.23:9092", "34.194.141.114:9092"]
TOPIC = "wikimedia"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id='wikimedia-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    key_deserializer=lambda x: x.decode('utf-8') if x else None
)

# Initialize OpenSearch client
Client = OpenSearch(
    hosts=["http://localhost:9200"], 
    http_compress=True,  
    timeout=30,  
    retries=5,
    use_ssl=False,
    verify_certs=False,
    ssl_assert_hostname=False,
    ssl_show_warn=False
)

INDEX_NAME = "wikimedia_changes"
index_body = {
    'settings': {
        'index': {
            'number_of_shards': 3,
            'number_of_replicas': 2
        }
    }
}

try:
    if not Client.indices.exists(index=INDEX_NAME):
        Client.indices.create(index=INDEX_NAME, body=index_body)
except Exception as e:
    logger.error(f"Error checking/creating index: {e}")

def process_message(event):
    try:
        timestamp = event.get('timestamp')
        category = event.get('title')
        user = event.get('user')
        comment = event.get('parsedcomment')
        
        document = {
            "timestamp": timestamp,
            "category": category,
            "user": user,
            "comment": comment,
            "raw_event": event  
        }

        response = Client.index(index=INDEX_NAME, body=document)
        logger.info(f"Indexed event with response: {response['result']}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

def consume_kafka():
    try:
        logger.info(f"Consuming messages from Kafka topic: {TOPIC}")
        for message in consumer:
            event = message.value
            logger.info(f"Processing event: {event.get('id')}")
            process_message(event)

    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    consume_kafka()
