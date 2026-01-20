package com.example.flinkcassandra.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public class CassandraUtils {

    private static CqlSession session;

    public static void createSession(String contactPoint, int port) {
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .build();
    }

    public static void closeSession() {
        if (session != null) {
            session.close();
        }
    }

    public static void executeQuery(String query) {
        if (session != null) {
            session.execute(SimpleStatement.newInstance(query));
        } else {
            throw new IllegalStateException("Cassandra session is not established.");
        }
    }

    public static CqlSession getSession() {
        return session;
    }
}