#!/bin/sh
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-bnpb.hazard-events.v1}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "waiting for kafka at $BOOTSTRAP"
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor 1 \
  --config retention.ms="$RETENTION_MS" \
  --config cleanup.policy=delete

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
