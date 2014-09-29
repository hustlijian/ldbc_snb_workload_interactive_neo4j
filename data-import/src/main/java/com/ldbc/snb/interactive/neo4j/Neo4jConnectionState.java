package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Neo4jConnectionState extends DbConnectionState {
    private final GraphDatabaseService db;
    private final ExecutionEngine executionEngine;
    private final LdbcTraversers traversers;
    private final Connection connection;

    public Neo4jConnectionState(GraphDatabaseService db,
                                ExecutionEngine executionEngine,
                                LdbcTraversers traversers,
                                String url) throws DbException {
        this.db = db;
        this.executionEngine = executionEngine;
        this.traversers = traversers;
        try {
            if (null == url) {
                this.connection = null;
            } else {
                this.connection = DriverManager.getConnection(url);
                this.connection.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new DbException("Error while creation JDBC database connection", e);
        }
    }

    public GraphDatabaseService db() {
        return db;
    }

    public ExecutionEngine executionEngine() {
        return executionEngine;
    }

    public LdbcTraversers traversers() {
        return traversers;
    }

    public Connection connection() throws SQLException {
        return connection;
    }

    public void shutdown() throws DbException {
        if (null != connection) try {
            connection.close();
        } catch (SQLException e) {
            throw new DbException("Error while closing JDBC connection", e);
        }
        if (null != db) {
            db.shutdown();
        }
    }
}
