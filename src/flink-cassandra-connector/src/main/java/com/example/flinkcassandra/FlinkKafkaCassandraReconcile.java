package com.example.flinkcassandra;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.example.flinkcassandra.utils.CassandraClusterBuilder;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static org.junit.jupiter.api.DynamicTest.stream;

import java.net.InetSocketAddress;
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

        // Process each message and send to 'reconsile' topic
        DataStream<String> processedStream = kafkaStream.map(message -> {
            // Extract the primary key name, value, keyspace, and table from the message
            String pkName = extractFieldFromMessage(message, "partitionColumns", "name", "PK");
            String pkValue = extractFieldFromMessage(message, "partitionColumns", "value", "PK");
            String keyspace = extractSimpleField(message, "keyspace");
            String table = extractSimpleField(message, "table");

            // Query Cassandra using the extracted values
            Row cassandraRow = queryCassandra(keyspace, table, pkName, pkValue);

            // Hash the input Kafka message and Cassandra row
            String inputHash = hashString(message);
            String cassandraHash = hashRow(cassandraRow);

            // Create a JSON message with the required fields
            String payload = cassandraRow != null ? cassandraRow.toString() : "null";
            String timestamp = String.valueOf(System.currentTimeMillis());
            String action = extractSimpleField(message, "action"); // Assuming 'action' field exists in the message

            return String.format(
                "{\"pk\":\"%s\", \"hash\":\"%s\", \"cass_hash\":\"%s\", \"payload\":\"%s\", \"ts\":\"%s\", \"action\":\"%s\"}",
                pkValue, inputHash, cassandraHash, payload, timestamp, action
            );
        });

        CassandraSink.addSink(processedStream)
            .setClusterBuilder(new CassandraClusterBuilder())
            .setQuery("INSERT INTO ks.table (id, value) VALUES (?, ?);")
            .build();
        // Send the processed data to the 'reconsile' Kafka topic
        processedStream.addSink(kafkaProducer);

        // Produce example Kafka messages to the "input-topic"
        produceKafkaMessages("input-topic", kafkaProducerProps);

        // Execute the Flink job
        env.execute("Flink Kafka Cassandra Reconcile Job");
    }

    @Override
    protected CqlSessionBuilder buildSession(CqlSessionBuilder builder) {
        return builder
            .addContactPoint(new InetSocketAddress("node1", 9042))
            .addContactPoint(new InetSocketAddress("node2", 9042))
            .withLocalDatacenter("dc1");
    }

    private static String extractFieldFromMessage(String message, String arrayField, String targetField, String typeValue) {
        // Extract a specific field from a JSON array based on a type value
        String pattern = String.format(".*\"%s\"\\s*:\\s*\\[.*?\"type\"\\s*:\\s*\"%s\".*?\"%s\"\\s*:\\s*\"(.*?)\".*?\\].*", arrayField, typeValue, targetField);
        if (message.matches(pattern)) {
            return message.replaceAll(pattern, "$1");
        }
        return null;
    }

    private static String extractSimpleField(String message, String fieldName) {
        // Extract a simple field from the JSON message
        String pattern = String.format(".*\"%s\"\\s*:\\s*\"(.*?)\".*", fieldName);
        if (message.matches(pattern)) {
            return message.replaceAll(pattern, "$1");
        }
        return null;
    }

    private static Row queryCassandra(String keyspace, String table, String pkName, String pkValue) {
        try (CqlSession session = CqlSession.builder().build()) {
            String query = String.format("SELECT * FROM %s.%s WHERE %s = '%s';", keyspace, table, pkName, pkValue);
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

    private static String hashString(String str) throws Exception {
        if (str == null) {
            return "null";
        }

        // Hash the string using SHA-256
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(str.getBytes(StandardCharsets.UTF_8));

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