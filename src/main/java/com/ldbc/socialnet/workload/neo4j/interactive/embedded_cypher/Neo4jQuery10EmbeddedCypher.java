package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery10;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery10EmbeddedCypher extends Neo4jQuery10<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String HOROSCOPE_MONTH_MIN_STRING = HOROSCOPE_MONTH_MIN.toString();
    protected static final String HOROSCOPE_MONTH_MAX_STRING = HOROSCOPE_MONTH_MAX.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();


    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery10Result> execute(ExecutionEngine engine, LdbcQuery10 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery10Result>() {
                    @Override
                    public LdbcQuery10Result apply(Map<String, Object> row) {
                        return new LdbcQuery10Result(
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                (long) row.get("personId"),
                                (double) row.get("commonInterestScore"),
                                (String) row.get("personGender"),
                                (String) row.get("personCityName"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery10 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(HOROSCOPE_MONTH_MIN_STRING, operation.month1());
        queryParams.put(HOROSCOPE_MONTH_MAX_STRING, operation.month2());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
