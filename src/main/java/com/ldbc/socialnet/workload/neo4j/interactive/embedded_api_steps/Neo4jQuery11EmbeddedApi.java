package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery11Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery11;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery11EmbeddedApi implements Neo4jQuery11 {
    private final LdbcTraversers traversers;

    public Neo4jQuery11EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query11 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery11Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery11 operation) {
        return null;
    }
}
