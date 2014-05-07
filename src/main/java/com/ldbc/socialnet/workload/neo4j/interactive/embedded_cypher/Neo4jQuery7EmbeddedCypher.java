package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery7;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery7EmbeddedCypher implements Neo4jQuery7 {
    private static final String QUERY_STRING = ""
            + "MATCH (start:" + Nodes.Person + " {" + Person.ID + ":{person_id}})<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")<-[like:" + Rels.LIKES + "]-(person:" + Nodes.Person + ")\n"
            + "RETURN person." + Person.ID + " AS personId, person." + Person.FIRST_NAME + " AS personFirstName, person." + Person.LAST_NAME + " AS personLastName,"
            + " like." + Likes.CREATION_DATE + " AS likeDate,  NOT((person)-[:" + Rels.KNOWS + "]-(start)) AS isNew, post." + Post.ID + " AS postId,"
            + " post." + Post.CONTENT + " AS postContent, like." + Likes.CREATION_DATE + " - post." + Post.CREATION_DATE + " AS latency\n"
            + "ORDER BY like." + Likes.CREATION_DATE + " DESC, personId ASC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery7Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery7 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery7Result>() {
                    @Override
                    public LdbcQuery7Result apply(Map<String, Object> row) {
                        return new LdbcQuery7Result(
                                (long) row.get("personId"),
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                new Date((long) row.get("likeDate")),
                                (boolean) row.get("isNew"),
                                (long) row.get("postId"),
                                (String) row.get("postContent"),
                                (long) row.get("latency"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery7 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
