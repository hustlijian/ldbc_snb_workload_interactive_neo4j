package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery9Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery9;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery9EmbeddedCypher implements Neo4jQuery9 {
    /*
        Q9 - Find the newest 20 posts from your friends within 2 steps
        Description
            Find the newest 20 posts and comments from all of your friends, or from friends of your friends (excluding you), but created before a certain date (excluding that date).
            Id of a friend, Id of a post, and creationDate are returned, sorted descending by creationDate, and then ascending by post URI.
        Parameter
            Person
            Date0
        Result (for each result return)
            Person.Id
            Person.firstName
            Person.lastName
            Post.Id or Comment.Id
            Post.content or Post.imageFile or Comment.content
            Post.creationDate or Comment.creationDate
     */

    private static final String QUERY_STRING = ""
            + "MATCH (:" + Nodes.Person + " {" + Person.ID + ":{person_id}})-[:" + Rels.KNOWS + "*1..2]-(friend:" + Nodes.Person + ")\n"
            + "MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(activity) WHERE activity." + Post.CREATION_DATE + " < {latest_date}\n"
            + "RETURN DISTINCT"
            + " activity." + Post.ID + " AS activityId,"
            + " activity." + Post.CONTENT + " AS activityContent,"
            + " activity." + Post.CREATION_DATE + " AS activityCreationDate,"
            + " friend." + Person.ID + " AS personId,"
            + " friend." + Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Person.LAST_NAME + " AS personLastName\n"
            + "ORDER BY activity.creationDate DESC, activity.id ASC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery9Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery9 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery9Result>() {
                    @Override
                    public LdbcQuery9Result apply(Map<String, Object> row) {
                        return new LdbcQuery9Result(
                                (long) row.get("personId"),
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                (long) row.get("activityId"),
                                (String) row.get("activityContent"),
                                (long) row.get("activityCreationDate"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery9 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("latest_date", operation.date());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
