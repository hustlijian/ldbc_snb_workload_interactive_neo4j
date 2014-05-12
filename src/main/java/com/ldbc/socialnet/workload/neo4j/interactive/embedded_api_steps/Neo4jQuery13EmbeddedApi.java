package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery13Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery13;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery13EmbeddedApi implements Neo4jQuery13 {
    private final LdbcTraversers traversers;

    public Neo4jQuery13EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query13 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery13Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery13 operation) {
        return null;
    }
}
