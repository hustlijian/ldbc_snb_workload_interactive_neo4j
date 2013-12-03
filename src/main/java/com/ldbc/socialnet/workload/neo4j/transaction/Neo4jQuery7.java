package com.ldbc.socialnet.workload.neo4j.transaction;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7Result;

public interface Neo4jQuery7 extends Neo4jQuery<LdbcQuery7, LdbcQuery7Result>
{
    /*
    QUERY 7
    
    DESCRIPTION
        Find the 10 most popular tags that occurred in your country during the last X hours, 
        and that none of your friends has discussed.

    FORMALLY
        Find posts in person's country
        Made in last X hours
        That person's friends have not commented on
        Get tags on those posts
        Order tags by "popularity" (count)
        Limit to 10 most popular

    PARAMETERS
        Person
        startDateTime
        duration (hours)
                    
    RETURN
        Tag.name
        count        
     */
}
