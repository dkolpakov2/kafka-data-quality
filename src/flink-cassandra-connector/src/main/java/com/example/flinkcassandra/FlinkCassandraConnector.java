package com.example.flinkcassandra;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

public class FlinkCassandraConnector {

    private CqlSession session;
    private String keyspace;

    public FlinkCassandraConnector(String keyspace) {
        this.keyspace = keyspace;
        this.session = createCassandraSession();
    }

    private CqlSession createCassandraSession() {
        // Implement logic to create and return a Cassandra session
        return CqlSession.builder().build();
    }

    public void executeQuery(String query) {
        session.execute(query);
    }

    public void addDataStream(DataStream<Tuple2<String, String>> dataStream) {
        dataStream.map(tuple -> {
            // Query Cassandra using primary key and value
            String primaryKey = tuple.f0; // data1
            String primaryKeyValue = tuple.f1; // data2
            String query = "SELECT * FROM " + keyspace + ".your_table WHERE " + primaryKey + " = '" + primaryKeyValue + "';";
            return session.execute(query).one(); // Fetch the first row (if any)
        }).addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row row, Context context) {
                if (row != null) {
                    // Process the row (example: print to console)
                    System.out.println("Fetched row: " + row);
                } else {
                    System.out.println("No data found for the given primary key and value.");
                }
            }
        });
    }

    public void addDataStream(DataStream<List<String>> dataStream) {
        dataStream.map(list -> {
            if (list.size() < 2) {
                throw new IllegalArgumentException("List must contain at least a primary key and its value.");
            }

            // Extract primary key and values
            String primaryKey = list.get(0); // First element is the primary key
            String primaryKeyValue = list.get(1); // Second element is the primary key value

            // Construct query dynamically
            StringBuilder queryBuilder = new StringBuilder("SELECT * FROM ")
                .append(keyspace)
                .append(".your_table WHERE ")
                .append(primaryKey)
                .append(" = '")
                .append(primaryKeyValue)
                .append("'");

            // Add additional conditions if more elements exist
            for (int i = 2; i < list.size(); i += 2) {
                queryBuilder.append(" AND ")
                    .append(list.get(i)) // Column name
                    .append(" = '")
                    .append(list.get(i + 1)) // Column value
                    .append("'");
            }

            String query = queryBuilder.toString();
            return session.execute(query).one(); // Fetch the first row (if any)
        }).addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row row, Context context) {
                if (row != null) {
                    // Process the row (example: print to console)
                    System.out.println("Fetched row: " + row);
                } else {
                    System.out.println("No data found for the given query.");
                }
            }
        });
    }

    public void close() {
        if (session != null) {
            session.close();
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkCassandraConnector connector = new FlinkCassandraConnector("your_keyspace");

        // Example data stream
        DataStream<Tuple2<String, String>> dataStream = env.fromElements(
            Tuple2.of("data1", "value1"),
            Tuple2.of("data2", "value2"),
            Tuple2.of("data3", "value3")
        );
        connector.addDataStream(dataStream);

        env.execute("Flink Cassandra Connector Job");
        connector.close();
    }
}