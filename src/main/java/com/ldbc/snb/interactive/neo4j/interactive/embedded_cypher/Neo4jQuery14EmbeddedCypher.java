package com.ldbc.snb.interactive.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery14;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery14EmbeddedCypher extends Neo4jQuery14<ExecutionEngine> {
    protected static final String PERSON_ID_1_STRING = PERSON_ID_1.toString();
    protected static final String PERSON_ID_2_STRING = PERSON_ID_2.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(ExecutionEngine engine, LdbcQuery14 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery14Result>() {
                    @Override
                    public LdbcQuery14Result apply(Map<String, Object> row) {
                        return new LdbcQuery14Result(
                                (Collection<Long>) row.get("pathNodeIds"),
                                (double) row.get("weight"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery14 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_1_STRING, operation.person1Id());
        queryParams.put(PERSON_ID_2_STRING, operation.person2Id());
        return queryParams;
    }
}
