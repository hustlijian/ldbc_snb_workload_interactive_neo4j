package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7Result;

public interface Neo4jQuery7 extends Neo4jQuery<LdbcQuery7, LdbcQuery7Result>
{
    /*
    QUERY 7

    Description
        For the specified person, this query gets the most recent likes of any of the person’s posts.
        Likes from outside the direct connections are specially gratifying, hence these are flagged.
        The latency between the post and the like is also reported. Speed of reaction is a potential correlate of interest.
        The results are ordered descending by creation date of the like, and then ascending by liker URI, and the top 20 are returned.
    Parameter
        Person
    Result (for each result return)
        Person.id (for the person who likes the post)
        Person.firstName (for the person who likes the post)
        Person.lastName (for the person who likes the post)
        CreationDate of the like
        is_new (0 if person who liked the post is friend of the author of the post, 1 otherwise)
        Post.id
        Post.content or Post.imageFile
        lag - latency between the post and the like in minutes
     */

}
