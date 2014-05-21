package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery12;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery12EmbeddedApi extends Neo4jQuery12<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery12EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query12 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery12Result> execute(GraphDatabaseService db, LdbcQuery12 operation) {
        return null;
    }
}
