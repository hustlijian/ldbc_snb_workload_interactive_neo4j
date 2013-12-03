package com.ldbc.socialnet.workload.neo4j.transaction;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6Result;

public interface Neo4jQuery6 extends Neo4jQuery<LdbcQuery6, LdbcQuery6Result>
{
    /*
    QUERY 6

    STORY 
    
        People who discuss X also discuss.
        Find 10 most popular Tags of people that are connected to you via friendship path and talk about topic/Tag 'X'.
    
    DESCRIPTION

        Among POSTS by FRIENDS and FRIENDS OF FRIENDS, find the TAGS most commonly occurring together with a given TAG.

    PARAMETERS
    
        Person
        Source.tag
                    
    RETURN
    
        Tag.name
        count
     */
}
