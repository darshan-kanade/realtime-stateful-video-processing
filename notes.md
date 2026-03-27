# Kafka Setup

## Step 0: Enter Kafka Dir -> /home/darsh/kafka_2.13-4.2.0

## Step 1:
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties

## Next Steps:
### Start Server
bin/kafka-server-start.sh -daemon config/server.properties

### Create Topic
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092

### Stop Server
bin/kafka-server-stop.sh
