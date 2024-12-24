python3 -m venv kafka-env
source kafka-env/bin/activate

#Start Zookeeper 
bin/zookeeper-server-start.sh config/zookeeper.properties

#Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

#Create a Topic
bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3 --replication-factor 1
