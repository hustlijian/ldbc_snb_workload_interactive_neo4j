package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery8Result;

public interface Neo4jQuery8 extends Neo4jQuery<LdbcQuery8, LdbcQuery8Result>
{
    /*
    QUERY 8 - Most recent replies

    DESCRIPTION
        This query retrieves the most recent (20) replies to all the posts and comments of the specified person.
        Order them descending by creation date, and then ascending by reply URI.
    PARAMETERS
        Person
    RETURN
        Person.id (of the person who replied to the post)
        Person.firstName (of the person who replied to the post)
        Person.lastName (of the person who replied to the post)
        CreationDate of the reply
        Reply URI
        Content of the reply
     */
}
