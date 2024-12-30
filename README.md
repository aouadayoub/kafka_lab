python3 -m venv kafka-env
source kafka-env/bin/activate

#Start Zookeeper 
bin/zookeeper-server-start.sh config/zookeeper.properties

#Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

#Create a Topic
bin/kafka-topics.sh --create --topic wikimedia --bootstrap-server 52.6.122.23:9092,34.194.141.114:9092 --partitions 3 --replication-factor 2

#Kafka Ui 
http://localhost:9000/

cd kafka_labs
python3 producer.py
python3 consumer.py

##OpenSearch
#Follow https://green.cloud/docs/how-to-install-opensearch-on-ubuntu-20-04/

Replace `sudo env OPENSEARCH_INITIAL_ADMIN_PASSWORD=<custom-admin-password> dpkg -i opensearch-2.18.0-linux-x64.deb` with :
 `sudo env OPENSEARCH_INITIAL_ADMIN_PASSWORD=<custom-admin-password> apt install opensearch`

### NOT starting on installation, please execute the following statements to configure opensearch service to start automatically using systemd
 sudo systemctl daemon-reload
 sudo systemctl enable opensearch.service
### You can start opensearch service by executing
 sudo systemctl start opensearch.service

 verfiy connection to opensearch : 
 curl -X GET https://localhost:9200 -u 'admin:<custom-admin-password>' --insecure
