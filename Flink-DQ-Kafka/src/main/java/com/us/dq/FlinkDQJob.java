package com.us.dq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import java.util.Properties;

public class FlinkDQJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();
        // Kafka consumer configuration
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", "flink-dq");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "validated_input",
                new SimpleStringSchema(),
                props
        );
        // Ingest raw JSON strings
        var stream = env.addSource(consumer);
        // Parse JSON
        var parsed = stream.map((MapFunction<String, JsonNode>) value -> {
            try {
                return mapper.readTree(value);
            } catch (Exception e) {
                return null;
            }
        });
        // Filter null or malformed JSON
        var validJson = parsed.filter((FilterFunction<JsonNode>) node -> node != null);
        // Basic DQ rule: value must be >= 0
        var cleaned = validJson.filter(
                (FilterFunction<JsonNode>) node -> node.has("value")
                        && node.get("value").asDouble() >= 0
        );

        // Convert to tuple for Cassandra sink
        var tuples = cleaned.map(
                (MapFunction<JsonNode, Tuple3<String, String, Double>>) node ->
                        Tuple3.of(
                                node.get("id").asText(),
                                node.get("timestamp").asText(),
                                node.get("value").asDouble()
                        )
        );
// // After cleaned DataStream<JsonNode> cleaned
// DataStream<Tuple3<String, String, Double>> tuples = cleaned.map(...);

// // Key by id (or topic) and window
// DataStream<String> stats = tuples
//     .keyBy(t -> t.f0) // id
//     .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
//     .aggregate(new StatsAggregate(), new AnomalyProcess());

// // stats contains mean/std per key. You can broadcast this to compute per-event z-score, or
// // alternatively compute z-score in ProcessFunction with stateful aggregates per key (preferred for per-event).
        // Cassandra sink
        CassandraSink.addSink(tuples)
                .setQuery("INSERT INTO dq.data (id, ts, value) VALUES (?, ?, ?);")
                .setHost("cassandra")
                .build();

        env.execute("Kafka → Flink → Cassandra Data Quality Pipeline");
    }
}
