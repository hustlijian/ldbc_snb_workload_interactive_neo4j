package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery13Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery13;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery13EmbeddedCypher implements Neo4jQuery13 {
    private static final String QUERY_STRING = ""
            + "RETURN 0";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery13Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery13 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery13Result>() {
                    @Override
                    public LdbcQuery13Result apply(Map<String, Object> row) {
                        // TODO
                        return new LdbcQuery13Result(0);
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery13 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        // TODO
        return queryParams;
    }
}
