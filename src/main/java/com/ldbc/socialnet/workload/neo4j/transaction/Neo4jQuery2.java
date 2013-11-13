package com.ldbc.socialnet.workload.neo4j.transaction;

import com.ldbc.socialnet.workload.LdbcQuery2;
import com.ldbc.socialnet.workload.LdbcQuery2Result;

public interface Neo4jQuery2 extends Neo4jQuery<LdbcQuery2, LdbcQuery2Result>
{
    /*
    QUERY2:
        Find the newest 20 posts from your friends
        
        Find the newest 20 posts from all of your friends, but created before a certain date (including that date). 
        ID of a friend, ID of a post, and creationDate are returned, sorted descending by creationDate, and then ascending by post URI.
        
    FORMALLY:
        Get friends
        Get posts by friends that were made before a certain date (including that date)
        Order by post creation date
        Limit to top 20

    PARAMETERS:
        Person
        Date

    RETURN:    
        Person.Id
        Person.firstName
        Person.lastName
        Post.Id
        Post.content or Post.imageFile
        Post.Date
     */
}
