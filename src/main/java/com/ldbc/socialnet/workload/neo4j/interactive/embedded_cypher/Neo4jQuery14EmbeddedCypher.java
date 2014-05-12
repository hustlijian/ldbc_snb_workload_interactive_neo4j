package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery14;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery14EmbeddedCypher implements Neo4jQuery14 {
    private static final String QUERY_STRING = ""
            + "RETURN 0";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery14 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery14Result>() {
                    @Override
                    public LdbcQuery14Result apply(Map<String, Object> row) {
                        // TODO
                        return new LdbcQuery14Result(new ArrayList<Long>());
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery14 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        // TODO
        return queryParams;
    }
}
