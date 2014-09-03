package com.ldbc.socialnet.neo4j.workload;

import org.neo4j.graphdb.GraphDatabaseService;

import java.sql.Connection;

public class JdbcConnectionState {
    private final Connection connection;
    private final GraphDatabaseService db;

    public JdbcConnectionState(Connection connection, GraphDatabaseService db) {
        this.connection = connection;
        this.db = db;
    }

    public Connection connection() {
        return connection;
    }

    public GraphDatabaseService db() {
        return db;
    }
}
