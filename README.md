python3 -m venv kafka-env
source kafka-env/bin/activate

#Start Zookeeper 
bin/zookeeper-server-start.sh config/zookeeper.properties

#Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

#Create a Topic
bin/kafka-topics.sh --create --topic wikimedia --bootstrap-server 52.6.122.23:9092,34.194.141.114:9092 --partitions 3 --replication-factor 2

cd kafka_labs
python3 producer.py
python3 consumer.py