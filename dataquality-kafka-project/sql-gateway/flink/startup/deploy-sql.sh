#!/bin/bash
set -e
GATEWAY_ENDPOINT="localhost:8083"
FLINK_SQL_CLIENT="/opt/flink/bin/sql-client.sh"  #--jar /opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar
GATEWAY="gateway"
SQL_CLIENT="/opt/flink/bin/sql-client.sh"
echo " Deploying Flink DQ SQL..."
# Add required JARs explicitly
FLINK_LIB_DIR="/opt/flink/lib"
KAFKA_CONNECTOR_JAR="$FLINK_LIB_DIR/flink-sql-connector-kafka-1.17.1.jar"
JSON_FORMAT_JAR="$FLINK_LIB_DIR/flink-json-1.17.1.jar"
export FLINK_SQL_CLIENT="$FLINK_SQL_CLIENT --jar $KAFKA_CONNECTOR_JAR --jar $JSON_FORMAT_JAR"

$SQL_CLIENT gateway \
  -e "$GATEWAY_ENDPOINT" \
  -f "/opt/flink/sql/deploy.sql" \
  --jar "$KAFKA_CONNECTOR_JAR" --jar /opt/flink/lib/flink-json-1.17.1.jar
  # --jar ${JSON_FORMAT_JAR}

# $SQL_CLIENT gateway \
#   -e ${GATEWAY_ENDPOINT} \
#   -f /opt/flink/sql/02_enrichment.sql

# $SQL_CLIENT gateway \
#   -e ${GATEWAY_ENDPOINT} \
#   -f /opt/flink/dq/dq_generated.sql

# $SQL_CLIENT gateway \
#   -e ${GATEWAY_ENDPOINT} \
#   -f /opt/flink/sql/03_dq_rules.sql

# $SQL_CLIENT gateway \
#   -e ${GATEWAY_ENDPOINT} \
#   -f /opt/flink/sql/04_metrics.sql

# $SQL_CLIENT gateway \
#   -e ${GATEWAY_ENDPOINT} \
#   -f /opt/flink/sql/05_sinks.sql

# $FLINK_SQL_CLIENT $GATEWAY -f /opt/flink/sql/01_sources.sql
# $FLINK_SQL_CLIENT $GATEWAY -f /opt/flink/sql/02_enrichment.sql
# $FLINK_SQL_CLIENT $GATEWAY -f /opt/flink/dq/dq_generated.sql
# $FLINK_SQL_CLIENT $GATEWAY -f /opt/flink/sql/04_metrics.sql
# $FLINK_SQL_CLIENT $GATEWAY -f /opt/flink/sql/05_sinks.sql

echo " DQ SQL deployed successfully"
