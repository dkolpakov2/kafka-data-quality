package com.example.flinkcassandra;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Properties;

public class FlinkKafkaCassandraReconcile {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer properties
        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaConsumerProps.setProperty("group.id", "flink-group");

        // Kafka producer properties
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty("bootstrap.servers", "localhost:9092");

        // Create Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "input-topic",
            new SimpleStringSchema(),
            kafkaConsumerProps
        );

        // Create Kafka producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
            "reconsile",
            new SimpleStringSchema(),
            kafkaProducerProps
        );

        // Read messages from Kafka
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Process each message
        DataStream<String> processedStream = kafkaStream.map(message -> {
            // Extract the primary key (pk) from the message
            String pk = extractPrimaryKey(message);

            // Query Cassandra using the pk
            Row cassandraRow = queryCassandra(pk);

            // Hash the result
            String hashedResult = hashRow(cassandraRow);

            return hashedResult;
        });

        // Send the processed data to the "reconsile" Kafka topic
        processedStream.addSink(kafkaProducer);

        // Produce example Kafka messages to the "input-topic"
        produceKafkaMessages("input-topic", kafkaProducerProps);

        // Execute the Flink job
        env.execute("Flink Kafka Cassandra Reconcile Job");
    }

    private static String extractPrimaryKey(String message) {
        // Extract the primary key (pk) from the message
        // Assuming the message is a JSON string with a "pk" field
        // Example: {"pk": "value"}
        return message.replaceAll(".*\"pk\":\"(.*?)\".*", "$1");
    }

    private static Row queryCassandra(String pk) {
        try (CqlSession session = CqlSession.builder().build()) {
            String query = "SELECT * FROM your_keyspace.your_table WHERE pk = '" + pk + "';";
            return session.execute(query).one();
        }
    }

    private static String hashRow(Row row) throws Exception {
        if (row == null) {
            return "null";
        }

        // Convert the row to a string representation
        String rowString = row.toString();

        // Hash the string using SHA-256
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(rowString.getBytes(StandardCharsets.UTF_8));

        // Convert the hash bytes to a hexadecimal string
        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }

    private static void produceKafkaMessages(String topic, Properties kafkaProducerProps) {
        // Create Kafka producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
            topic,
            new SimpleStringSchema(),
            kafkaProducerProps
        );

        // Example messages to produce
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> messageStream = env.fromElements(
            "{\"pk\":\"value1\"}",
            "{\"pk\":\"value2\"}",
            "{\"pk\":\"value3\"}"
        );

        // Add the producer to the stream
        messageStream.addSink(kafkaProducer);

        // Add the new JSON message to the Kafka producer
        DataStream<String> newMessageStream = env.fromElements(
            "{\"source\":\"source1\",\"target\":\"target1\",\"table\": \"table1\",\"keyspace\":\"keyspace1\",\"cql\":\"INSERT INTO keyspace1.table1 ()\",\"timestampNanos\":\"2025-01-01T00:00:00Z\",\"correlationId\":\"12345\",\"type\":\"TYPE\",\"metadata\":\"metadata\",\"statementPayload\":[{\"type\":\"TYPE\",\"table\":\"table1\",\"partitionColumns\":[{\"name\":\"name\", \"value\":\"value\", \"type\":\"type\"}]}]}"
        );

        newMessageStream.addSink(new FlinkKafkaProducer<>(
            "input-topic",
            new SimpleStringSchema(),
            kafkaProducerProps
        ));

        try {
            env.execute("Kafka Message Producer");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}