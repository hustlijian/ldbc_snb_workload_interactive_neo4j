package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery3;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery3EmbeddedCypher extends Neo4jQuery3<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String COUNTRY_X_STRING = COUNTRY_X.toString();
    protected static final String COUNTRY_Y_STRING = COUNTRY_Y.toString();
    protected static final String MIN_DATE_STRING = MIN_DATE.toString();
    protected static final String MAX_DATE_STRING = MAX_DATE.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery3Result> execute(ExecutionEngine engine, LdbcQuery3 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery3Result>() {
                    @Override
                    public LdbcQuery3Result apply(Map<String, Object> cypherResult) {
                        return new LdbcQuery3Result(
                                (long) cypherResult.get("friendId"),
                                (String) cypherResult.get("friendFirstName"),
                                (String) cypherResult.get("friendLastName"),
                                (long) cypherResult.get("xCount"),
                                (long) cypherResult.get("yCount"),
                                (long) cypherResult.get("xyCount"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery3 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(COUNTRY_X_STRING, operation.countryXName());
        queryParams.put(COUNTRY_Y_STRING, operation.countryYName());
        long startDateAsMilli = operation.startDate().getTime();
        int durationHours = operation.durationDays() * 24;
        long endDateAsMilli = Time.fromMilli(startDateAsMilli).plus(Duration.fromHours(durationHours)).asMilli();
        queryParams.put(MIN_DATE_STRING, startDateAsMilli);
        queryParams.put(MAX_DATE_STRING, endDateAsMilli);
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
