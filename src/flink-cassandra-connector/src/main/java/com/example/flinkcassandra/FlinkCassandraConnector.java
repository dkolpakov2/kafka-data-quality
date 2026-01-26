package com.example.flinkcassandra;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

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

    public void addDataStream(DataStream<String> dataStream) {
        dataStream.map(value -> {
            // Query Cassandra by primary key
            String query = "SELECT * FROM " + keyspace + ".your_table WHERE primary_key_column = '" + value + "';";
            return session.execute(query).one(); // Fetch the first row (if any)
        }).addSink(new SinkFunction<Row>() {
            @Override
            public void invoke(Row row, Context context) {
                if (row != null) {
                    // Process the row (example: print to console)
                    System.out.println("Fetched row: " + row);
                } else {
                    System.out.println("No data found for the given primary key.");
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
        DataStream<String> dataStream = env.fromElements("data1", "data2", "data3");
        connector.addDataStream(dataStream);

        env.execute("Flink Cassandra Connector Job");
        connector.close();
    }
}