#!/bin/sh
set -x

echo "Waiting for Kafka..."

while ! /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9093; do
  echo "Kafka not ready yet..."
  sleep 2
done

echo "Creating topic trades..."

/opt/kafka/bin/kafka-topics.sh --create \
  --topic trades \
  --bootstrap-server kafka:9093 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "Done!"