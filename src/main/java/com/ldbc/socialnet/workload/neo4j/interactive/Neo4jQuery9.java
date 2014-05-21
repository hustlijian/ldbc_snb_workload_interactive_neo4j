package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery9Result;

public abstract class Neo4jQuery9<CONNECTION> implements Neo4jQuery<LdbcQuery9, LdbcQuery9Result, CONNECTION> {

    /*
    QUERY 9 - Find the newest 20 posts from your friends within 2 steps
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
}
