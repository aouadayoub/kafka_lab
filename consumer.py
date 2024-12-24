from kafka import KafkaConsumer

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='python-group'
)

# Read messages from Kafka
print("Waiting for messages...")
for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
