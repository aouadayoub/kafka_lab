from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

topic = 'demo_java'
message = b'hello world'

producer.send(topic, message)

producer.flush()
producer.close()

print("Message sent successfully")
