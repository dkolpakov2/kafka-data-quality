package com.us.dq;

package com.example.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.us.dq.FlinkZScoreJob.CassandraRecord;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.functions.FilterFunction;

import java.util.Properties;
/* Complete Flink Job (Kafka → ZScore → Cassandra) */
public class FlinkZScoreJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        // Kafka Configuration
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "flink-dq");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "validated_input",
                new SimpleStringSchema(),
                props
        );

        // Ingest raw stream
        var raw = env.addSource(consumer);

        // Parse JSON
        var parsed = raw.map((MapFunction<String, JsonNode>) value -> {
            try {
                return mapper.readTree(value);
            } catch (Exception e) {
                return null;
            }
        });

        var valid = parsed.filter((FilterFunction<JsonNode>) n -> n != null && n.has("value"));

        // Key by ID → Each ID gets its own running stats
        var withZScore = valid
                .keyBy(n -> n.get("id").asText())
                .map(new ZScoreProcessFunction(3.0)); // z > 3 = anomaly

        // Convert to ingestable tuple for Cassandra
        var cassandraTuples = withZScore.map(
                (MapFunction<JsonNode, CassandraRecord>) node -> new CassandraRecord(
                        node.get("id").asText(),
                        node.get("timestamp").asText(),
                        node.get("value").asDouble(),
                        node.get("zscore").asDouble(),
                        node.get("isAnomaly").asBoolean()
                )
        );

        CassandraSink.addSink(cassandraTuples)
                .setHost("cassandra")
                .setQuery("INSERT INTO dq.dq_events (id, ts, value, zscore, is_anomaly) VALUES (?, ?, ?, ?, ?);")
                .build();

        env.execute("Flink Advanced DQ: Z-Score Detector");
    }

    // Simple POJO for Cassandra
    public static class CassandraRecord {
        public String id;
        public String ts;
        public double value;
        public double zscore;
        public boolean isAnomaly;

        public CassandraRecord() {}

        public CassandraRecord(String id, String ts, double value, double zscore, boolean isAnomaly) {
            this.id = id;
            this.ts = ts;
            this.value = value;
            this.zscore = zscore;
            this.isAnomaly = isAnomaly;
        }
    }
}
