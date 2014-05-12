package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery9Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery10;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery9;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery10EmbeddedApi implements Neo4jQuery10 {
    private final LdbcTraversers traversers;

    public Neo4jQuery10EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query10 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery10Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery10 operation) {
        return null;
    }
}
