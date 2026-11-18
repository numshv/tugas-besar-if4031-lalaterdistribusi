set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-bnpb.hazard-events.v1}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-86400000}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"
TOPICS="${KAFKA_TOPICS:-$TOPIC:$PARTITIONS:$RETENTION_MS}"

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "waiting for kafka at $BOOTSTRAP"
  sleep 2
done

for spec in $(echo "$TOPICS" | tr ',' ' '); do
  name="${spec%%:*}"
  rest="${spec#*:}"
  partitions="${rest%%:*}"
  retention="${rest#*:}"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION" \
    --config retention.ms="$retention" \
    --config cleanup.policy=delete
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$name"
done

# Compacted topics keep only the newest message per key (name:partitions:max_message_bytes).
for spec in $(echo "${KAFKA_COMPACTED_TOPICS:-}" | tr ',' ' '); do
  name="${spec%%:*}"
  rest="${spec#*:}"
  partitions="${rest%%:*}"
  maxbytes="${rest#*:}"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP"     --create --if-not-exists     --topic "$name"     --partitions "$partitions"     --replication-factor "$REPLICATION"     --config cleanup.policy=compact     --config max.message.bytes="$maxbytes"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$name"
done
