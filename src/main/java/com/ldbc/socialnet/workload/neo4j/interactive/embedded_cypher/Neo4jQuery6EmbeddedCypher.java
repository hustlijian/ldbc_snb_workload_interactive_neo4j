package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery6;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery6EmbeddedCypher extends Neo4jQuery6<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String TAG_NAME_STRING = TAG_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery6Result> execute(ExecutionEngine engine, LdbcQuery6 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery6Result>() {
                    @Override
                    public LdbcQuery6Result apply(Map<String, Object> next) {
                        return new LdbcQuery6Result(
                                (String) next.get("tagName"),
                                (long) next.get("tagCount"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery6 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(TAG_NAME_STRING, operation.tagName());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
