package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery1;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery1EmbeddedCypher extends Neo4jQuery1<ExecutionEngine> {
    private static final String PERSON_ID_STRING = PERSON_ID.toString();
    private static final String FRIEND_FIRST_NAME_STRING = FRIEND_FIRST_NAME.toString();
    private static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery1Result> execute(ExecutionEngine engine, LdbcQuery1 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery1Result>() {
                    @Override
                    public LdbcQuery1Result apply(Map<String, Object> row) {
                        return new LdbcQuery1Result(
                                (long) row.get("id"),
                                (String) row.get("lastName"),
                                (int) row.get("distance"),
                                (long) row.get("birthday"),
                                (long) row.get("creationDate"),
                                (String) row.get("gender"),
                                (String) row.get("browser"),
                                (String) row.get("locationIp"),
                                Lists.newArrayList((String[]) row.get("emails")),
                                Lists.newArrayList((String[]) row.get("languages")),
                                (String) row.get("cityName"),
                                (Collection<String>) row.get("unis"),
                                (Collection<String>) row.get("companies"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery1 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(FRIEND_FIRST_NAME_STRING, operation.firstName());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}