package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery8;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery8EmbeddedApi extends Neo4jQuery8<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery8EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query7 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery8Result> execute(GraphDatabaseService db, LdbcQuery8 operation) {
        return null;
    }
}
