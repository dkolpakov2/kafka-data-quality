package com.example.flinkcassandra.utils;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class CassandraClusterBuilder extends ClusterBuilder {

    @Override
    protected CqlSessionBuilder buildSession(CqlSessionBuilder builder) {
        return builder
            .addContactPoint(
                new InetSocketAddress("cassandra-host", 9042)
            )
            .withLocalDatacenter("datacenter1");
    }
}
