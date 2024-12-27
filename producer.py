import json
import requests
from kafka import KafkaProducer

KAFKA_BROKER = ["52.6.122.23:9092", "34.194.141.114:9092"]
TOPIC = "wikimedia"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8') if k else None,
    retries=5
)

url = "https://stream.wikimedia.org/v2/stream/recentchange" 



# Consume the stream and produce to Kafka
try:
    print("Starting Wikimedia stream...")
    with requests.get(url, stream=True) as response:
        # Check if the request was successful
        if response.status_code == 200:
            for line in response.iter_lines():
                if line:
                    line_str = line.decode('utf-8')
                    if line_str.startswith("data: "):
                        try:
                            data_json = line_str[6:]
                            data = json.loads(data_json)
                            print("Data:", data)
                            producer.send(TOPIC, value=data)
                            print("-" * 50)
                        except json.JSONDecodeError as e:
                            print(f"Failed to decode JSON from line: {line_str}")
                            print(f"Error: {e}")
        else:
            print(f"Failed to connect to the stream. Status code: {response.status_code}")

except requests.RequestException as e:
    print(f"An error occurred while connecting to the stream: {e}")
    
finally:
    producer.flush() 
    print("Producer closed.")
