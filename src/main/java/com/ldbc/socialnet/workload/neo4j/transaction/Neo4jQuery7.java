package com.ldbc.socialnet.workload.neo4j.transaction;

import com.ldbc.socialnet.workload.LdbcQuery7;
import com.ldbc.socialnet.workload.LdbcQuery7Result;

public interface Neo4jQuery7 extends Neo4jQuery<LdbcQuery7, LdbcQuery7Result>
{
    /*
    QUERY 7
    
    DESCRIPTION
        Find the 10 most popular tags that occurred in your country during the last X hours, and that none of your friends has discussed.

    PARAMETERS
        Person
        startDateTime
        duration (hours)
                    
    RETURN
        Tag.name
        count        
     */
}
