package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery14;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery14EmbeddedApi implements Neo4jQuery14 {
    private final LdbcTraversers traversers;

    public Neo4jQuery14EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query14 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery14 operation) {
        return null;
    }
}
