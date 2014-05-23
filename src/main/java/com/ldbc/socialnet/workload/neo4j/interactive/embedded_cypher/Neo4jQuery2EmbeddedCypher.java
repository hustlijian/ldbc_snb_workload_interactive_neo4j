package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery2;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery2EmbeddedCypher extends Neo4jQuery2<ExecutionEngine> {
    private static final String PERSON_ID_STRING = PERSON_ID.toString();
    private static final String MAX_DATE_STRING = MAX_DATE.toString();
    private static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery2Result> execute(ExecutionEngine engine, LdbcQuery2 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery2Result>() {
                    @Override
                    public LdbcQuery2Result apply(Map<String, Object> input) {
                        return new LdbcQuery2Result(
                                (long) input.get("personId"),
                                (String) input.get("personFirstName"),
                                (String) input.get("personLastName"),
                                (long) input.get("postId"),
                                (String) input.get("postContent"),
                                (long) input.get("postDate"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery2 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(MAX_DATE_STRING, operation.maxDateAsMilli());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
