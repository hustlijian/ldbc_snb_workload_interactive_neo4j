package com.ldbc.socialnet.workload.neo4j;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.ldbc.driver.DbConnectionState;

public class Neo4jConnectionStateEmbedded extends DbConnectionState
{
    private final GraphDatabaseService db;
    private final ExecutionEngine executionEngine;

    public Neo4jConnectionStateEmbedded( GraphDatabaseService db, ExecutionEngine executionEngine )
    {
        super();
        this.db = db;
        this.executionEngine = executionEngine;
    }

    public GraphDatabaseService getDb()
    {
        return db;
    }

    public ExecutionEngine getExecutionEngine()
    {
        return executionEngine;
    }
}
