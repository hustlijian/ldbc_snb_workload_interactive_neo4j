package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery14;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery14EmbeddedApi extends Neo4jQuery14<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery14EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query14 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(GraphDatabaseService db, LdbcQuery14 operation) {
        return null;
    }
}
