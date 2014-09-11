package com.ldbc.snb.interactive.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery12;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery12EmbeddedCypher extends Neo4jQuery12<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String TAG_CLASS_NAME_STRING = TAG_CLASS_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery12Result> execute(ExecutionEngine engine, LdbcQuery12 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery12Result>() {
                    @Override
                    public LdbcQuery12Result apply(Map<String, Object> row) {
                        return new LdbcQuery12Result(
                                (long) row.get("friendId"),
                                (String) row.get("friendFirstName"),
                                (String) row.get("friendLastName"),
                                (Collection<String>) row.get("tagNames"),
                                ((Long) row.get("count")).intValue());
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery12 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(TAG_CLASS_NAME_STRING, operation.tagClassName());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
