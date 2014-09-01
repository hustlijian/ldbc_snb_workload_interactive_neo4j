package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery7;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery7EmbeddedCypher extends Neo4jQuery7<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery7Result> execute(ExecutionEngine engine, LdbcQuery7 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery7Result>() {
                    @Override
                    public LdbcQuery7Result apply(Map<String, Object> row) {
                        long latencyAsMilli = (long) row.get("latencyAsMilli");
                        Long latencyAsMinutes = (latencyAsMilli / 1000) / 60;
                        return new LdbcQuery7Result(
                                (long) row.get("personId"),
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                (long) row.get("likeTime"),
                                (long) row.get("messageId"),
                                (String) row.get("messageContent"),
                                latencyAsMinutes.intValue(),
                                (boolean) row.get("isNew"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery7 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
