package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery5;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery5EmbeddedCypher extends Neo4jQuery5<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String JOIN_DATE_STRING = JOIN_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery5Result> execute(ExecutionEngine engine, LdbcQuery5 operation) {
        Map<String, Object> cypherParams = buildParams(operation);
        Function<Map<String, Object>, LdbcQuery5Result> transformFun = new Function<Map<String, Object>, LdbcQuery5Result>() {
            @Override
            public LdbcQuery5Result apply(Map<String, Object> row) {
                return new LdbcQuery5Result(
                        (String) row.get("forum"),
                        (long) row.get("postCount"));
            }
        };
        return Iterables.transform(engine.execute(QUERY_STRING, cypherParams), transformFun).iterator();
    }

    private Map<String, Object> buildParams(LdbcQuery5 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(JOIN_DATE_STRING, operation.minDate().getTime());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }

}
