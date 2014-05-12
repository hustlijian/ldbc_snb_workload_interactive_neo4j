package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery12;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery12EmbeddedCypher implements Neo4jQuery12 {
    /*
    Q12 - Expert Search
    Description
        Find friends of a specified user that have replied the most to posts with a tag in a given category.
        The result should be sorted descending by number of replies, and then ascending by friend's URI. Top 20 should be shown.
    Parameter
        Person
        Tag category
    Result (for each result return)
        Person.id
        Person.firstName
        Person.lastName
        Tag.name
        count
    */

    private static final String QUERY_STRING = ""
            + "";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery12Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery12 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery12Result>() {
                    @Override
                    public LdbcQuery12Result apply(Map<String, Object> row) {
                        // TODO remove
                        System.out.println(MapUtils.prettyPrint(row));
//                        return new LdbcQuery12Result(personId,personFirstName,personLastName,tagName,replyCount);
                        return null;
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery12 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("tag_class", operation.tagClass());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
