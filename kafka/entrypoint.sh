#!/bin/bash
set -e

# ✅ Pas de chown ici — Docker volume permet déjà l’écriture
echo "🔁 Checking KRaft log directory..."
mkdir -p /tmp/kraft-combined-logs

if [ ! -f "/tmp/kraft-combined-logs/meta.properties" ]; then
  CLUSTER_ID=$(kafka-storage random-uuid)
  echo "🆔 Generated CLUSTER_ID: $CLUSTER_ID"

  kafka-storage format \
    --ignore-formatted \
    --cluster-id "$CLUSTER_ID" \
    --config /etc/kafka/kafka.properties
else
  echo "✅ Cluster already formatted"
  # Read the existing CLUSTER_ID from meta.properties
  CLUSTER_ID=$(grep cluster.id /tmp/kraft-combined-logs/meta.properties | cut -d'=' -f2)
fi

export CLUSTER_ID

exec /etc/confluent/docker/run
