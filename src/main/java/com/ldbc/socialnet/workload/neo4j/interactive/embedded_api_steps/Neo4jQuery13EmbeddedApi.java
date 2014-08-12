package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery13;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;

public class Neo4jQuery13EmbeddedApi extends Neo4jQuery13<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery13EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query13 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery13Result> execute(GraphDatabaseService db, LdbcQuery13 operation) {
        return null;
    }
}
