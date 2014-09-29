package com.ldbc.snb.interactive.neo4j.interactive;

import org.neo4j.graphdb.GraphDatabaseService;

import java.sql.Connection;

public class JdbcFileConnectionState {
    private final Connection connection;
    private final GraphDatabaseService db;

    public JdbcFileConnectionState(Connection connection, GraphDatabaseService db) {
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
