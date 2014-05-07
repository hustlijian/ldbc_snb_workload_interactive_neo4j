package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery7;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery7EmbeddedApi implements Neo4jQuery7 {
    private final LdbcTraversers traversers;

    public Neo4jQuery7EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query7 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery7Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery7 operation) {
        return null;
    }
}
