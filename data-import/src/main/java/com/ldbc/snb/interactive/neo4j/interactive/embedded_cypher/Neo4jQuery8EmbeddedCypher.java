package com.ldbc.snb.interactive.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery8;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery8EmbeddedCypher extends Neo4jQuery8<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery8Result> execute(ExecutionEngine engine, LdbcQuery8 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery8Result>() {
                    @Override
                    public LdbcQuery8Result apply(Map<String, Object> row) {
                        return new LdbcQuery8Result(
                                (long) row.get("personId"),
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                (long) row.get("commentCreationDate"),
                                (long) row.get("commentId"),
                                (String) row.get("commentContent"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery8 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
