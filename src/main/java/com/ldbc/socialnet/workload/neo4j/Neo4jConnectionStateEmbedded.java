package com.ldbc.socialnet.workload.neo4j;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;

public class Neo4jConnectionStateEmbedded extends DbConnectionState
{
    private final GraphDatabaseService db;
    private final ExecutionEngine executionEngine;
    private final LdbcTraversers traversers;

    public Neo4jConnectionStateEmbedded( GraphDatabaseService db, ExecutionEngine executionEngine,
            LdbcTraversers traversers )
    {
        super();
        this.db = db;
        this.executionEngine = executionEngine;
        this.traversers = traversers;
    }

    public GraphDatabaseService db()
    {
        return db;
    }

    public ExecutionEngine executionEngine()
    {
        return executionEngine;
    }

    public LdbcTraversers traversers()
    {
        return traversers;
    }
}
