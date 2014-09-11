package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.snb.interactive.neo4j.Neo4jServerStarterStopper;
import org.neo4j.graphdb.GraphDatabaseService;

import java.sql.Connection;

public class JdbcUrlConnectionState {
    private final Connection connection;
    private final Neo4jServerStarterStopper neo4jServerStarterStopper;
    private final GraphDatabaseService db;

    public JdbcUrlConnectionState(Connection connection, Neo4jServerStarterStopper neo4jServerStarterStopper, GraphDatabaseService db) {
        this.connection = connection;
        this.neo4jServerStarterStopper = neo4jServerStarterStopper;
        this.db = db;
    }

    public Connection connection() {
        return connection;
    }

    public Neo4jServerStarterStopper neo4jServerStarter() {
        return neo4jServerStarterStopper;
    }

    public GraphDatabaseService db() {
        return db;
    }
}
