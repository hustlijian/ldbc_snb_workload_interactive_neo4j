package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery11;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery11EmbeddedCypher extends Neo4jQuery11<ExecutionEngine> {
    protected static final String PERSON_ID_STRING = PERSON_ID.toString();
    protected static final String WORK_FROM_YEAR_STRING = WORK_FROM_YEAR.toString();
    protected static final String COUNTRY_NAME_STRING = COUNTRY_NAME.toString();
    protected static final String LIMIT_STRING = LIMIT.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery11Result> execute(ExecutionEngine engine, LdbcQuery11 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery11Result>() {
                    @Override
                    public LdbcQuery11Result apply(Map<String, Object> row) {
                        return new LdbcQuery11Result(
                                (long) row.get("friendId"),
                                (String) row.get("friendFirstName"),
                                (String) row.get("friendLastName"),
                                (String) row.get("companyName"),
                                (int) row.get("workFromYear"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery11 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_STRING, operation.personId());
        queryParams.put(COUNTRY_NAME_STRING, operation.countryName());
        queryParams.put(WORK_FROM_YEAR_STRING, operation.workFromYear());
        queryParams.put(LIMIT_STRING, operation.limit());
        return queryParams;
    }
}
