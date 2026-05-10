#!/bin/bash

KAFKA_CONTAINER=kafka
BOOTSTRAP_SERVER=kafka:9092

topics=(
  "wiki-page-create"
  "breaking-news-alerts"
  "bot-alerts"
  "spam-alerts"
)

for topic in "${topics[@]}"; do
  docker exec -it $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --create \
    --if-not-exists \
    --topic $topic \
    --partitions 3 \
    --replication-factor 1
done

docker exec -it $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --list