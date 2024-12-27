from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers=["52.6.122.23:9092", "34.194.141.114:9092"],
    client_id='test'
)

topic_list = []
topic_list.append(NewTopic(name="wikimedia-top-final", num_partitions=3, replication_factor=2))

admin_client.create_topics(new_topics=topic_list, validate_only=False)

print("Topic created successfully")