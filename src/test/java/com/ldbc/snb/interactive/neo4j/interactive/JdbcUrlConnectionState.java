package com.ldbc.snb.interactive.neo4j.interactive;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.WrappingNeoServer;

import java.sql.Connection;

public class JdbcUrlConnectionState {
    private final Connection connection;
    private final WrappingNeoServer wrappingNeoServer;
    private final GraphDatabaseService db;

    public JdbcUrlConnectionState(Connection connection, WrappingNeoServer wrappingNeoServer, GraphDatabaseService db) {
        this.connection = connection;
        this.wrappingNeoServer = wrappingNeoServer;
        this.db = db;
    }

    public Connection connection() {
        return connection;
    }

    public WrappingNeoServer server() {
        return wrappingNeoServer;
    }

    public GraphDatabaseService db() {
        return db;
    }
}
