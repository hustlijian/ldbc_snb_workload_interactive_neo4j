package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery4;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery4EmbeddedCypher extends Neo4jQuery4<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String MIN_DATE_STRING = MIN_DATE.toString();
    protected static final String MAX_DATE_STRING = MAX_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery4Result> execute(ExecutionEngine engine, LdbcQuery4 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery4Result>() {
                    @Override
                    public LdbcQuery4Result apply(Map<String, Object> input) {
                        return new LdbcQuery4Result(
                                (String) input.get("tagName"),
                                (int) input.get("tagCount"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery4 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(MIN_DATE_STRING, operation.minDateAsMilli());
        queryParams.put(MAX_DATE_STRING, operation.maxDateAsMilli());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
