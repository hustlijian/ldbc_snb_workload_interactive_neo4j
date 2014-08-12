package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery14;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.*;

public class Neo4jQuery14EmbeddedCypher extends Neo4jQuery14<ExecutionEngine> {
    protected static final String PERSON_ID_1_STRING = PERSON_ID_1.toString();
    protected static final String PERSON_ID_2_STRING = PERSON_ID_2.toString();

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(ExecutionEngine engine, LdbcQuery14 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery14Result>() {
                    // TODO implement
                    @Override
                    public LdbcQuery14Result apply(Map<String, Object> row) {
                        // TODO old, remove
//                        Collection<LdbcQuery14Result.PathNode> pathNodes = Collections2.transform((Collection<Collection<Object>>) row.get("pathNodes"), new Function<Collection<Object>, LdbcQuery14Result.PathNode>() {
//                            @Override
//                            public LdbcQuery14Result.PathNode apply(Collection<Object> pathNode) {
//                                List<Object> pathNodeList = Lists.newArrayList(pathNode);
//                                return new LdbcQuery14Result.PathNode((String) pathNodeList.get(0), (long) pathNodeList.get(1));
//                            }
//                        });
//                        return new LdbcQuery14Result(
//                                pathNodes,
//                                (double) row.get("weight"));
                        // TODO temp, remove
                        Iterable<Long> personIdsInPath = null;
                        double pathWeight = 0;
                        return new LdbcQuery14Result(personIdsInPath, pathWeight);
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery14 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put(PERSON_ID_1_STRING, operation.person1Id());
        queryParams.put(PERSON_ID_2_STRING, operation.person2Id());
        return queryParams;
    }
}
