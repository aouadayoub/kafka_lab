import json
import logging
import os
from kafka import KafkaConsumer
from opensearchpy import OpenSearch
from dotenv import load_dotenv

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Charger les variables d'environnement
load_dotenv('/home/ubuntu/kafka_labs/.env')

# Configuration Kafka
kafka_topic = os.getenv('KAFKA_TOPIC', 'wikimedia')
kafka_broker = os.getenv('KAFKA_BROKER', ["52.6.122.23:9092", "34.194.141.114:9092"])

# Initialisation du consommateur Kafka
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    group_id='wikimedia-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    key_deserializer=lambda x: x.decode('utf-8') if x else None
)

# Authentification OpenSearch
auth = (os.getenv('OPENSEARCH_USERNAME'), os.getenv('OPENSEARCH_PASSWORD'))

# Initialisation du client OpenSearch
client = OpenSearch(
    hosts=["https://localhost:9200"], 
    http_compress=True,
    timeout=50,  
    max_retries=3, 
    retry_on_timeout=True,  
    use_ssl=True,  
    verify_certs=False,  
    ssl_assert_hostname=False, 
    ssl_show_warn=False,  
    http_auth=auth 
)

# Configuration des index
INDEX_NAME = "wikimedia_changes"
INDEX_NAME1 = "wikimedia_changes1"

index_body = {
    'settings': {
        'index': {
            'number_of_shards': 3,
            'number_of_replicas': 2
        }
    }
}

index_body1 = {
    'settings': {
        'index': {
            'number_of_shards': 3,
            'number_of_replicas': 2
        }
    },
    'mappings': {
        'properties': {
            'timestamp': {'type': 'date'},
            'category': {'type': 'keyword'},
            'user': {'type': 'keyword'},
            'comment': {'type': 'text'},
            'raw_event': {'type': 'object', 'enabled': False}
        }
    }
}

# Créer les index s'ils n'existent pas
try:
    if not client.indices.exists(index=INDEX_NAME):
        client.indices.create(index=INDEX_NAME, body=index_body)
    if not client.indices.exists(index=INDEX_NAME1):
        client.indices.create(index=INDEX_NAME1, body=index_body1)
except Exception as e:
    logger.error(f"Error checking/creating indices: {e}")

def process_message(event):
    try:
        timestamp = event.get('timestamp', 'Unknown')
        category = event.get('title', 'Unknown')
        user = event.get('user', 'Unknown')
        comment = event.get('parsedcomment', 'Unknown')

        # Créer le document à indexer
        document = {
            "timestamp": timestamp,
            "category": category.strip().lower(),
            "user": user,
            "comment": comment,
            "patrolled": event.get('patrolled', False),
            "raw_event": event  
        }

        # Déterminer quel index utiliser en fonction de certains critères
        index_to_use = INDEX_NAME1 if event.get('patrolled') == 'true' else INDEX_NAME

        response = client.index(index=index_to_use, body=document)
        logger.info(f"Indexed event in {index_to_use} with response: {response['result']}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")

def consume_kafka():
    try:
        logger.info(f"Consuming messages from Kafka topic: {kafka_topic}")
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