package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery9;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery9EmbeddedCypher extends Neo4jQuery9<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String LATEST_DATE_STRING = LATEST_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery9Result> execute(ExecutionEngine engine, LdbcQuery9 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery9Result>() {
                    @Override
                    public LdbcQuery9Result apply(Map<String, Object> row) {
                        return new LdbcQuery9Result(
                                (long) row.get("personId"),
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                (long) row.get("messageId"),
                                (String) row.get("messageContent"),
                                (long) row.get("messageCreationDate"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery9 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(LATEST_DATE_STRING, operation.maxDate().getTime());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
