package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery13;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery13EmbeddedCypher extends Neo4jQuery13<ExecutionEngine> {
    protected static final String PERSON_ID_1_STRING = PERSON_ID_1.toString();
    protected static final String PERSON_ID_2_STRING = PERSON_ID_2.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery13Result> execute(ExecutionEngine engine, LdbcQuery13 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery13Result>() {
                    @Override
                    public LdbcQuery13Result apply(Map<String, Object> row) {
                        return new LdbcQuery13Result(
                                ((Number) row.get("pathLength")).intValue());
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery13 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_1_STRING, operation.person1Id());
        queryParams.put(PERSON_ID_2_STRING, operation.person2Id());
        return queryParams;
    }
}
