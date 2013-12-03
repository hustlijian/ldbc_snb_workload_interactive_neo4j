package com.ldbc.socialnet.workload.neo4j.transaction;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4Result;

public interface Neo4jQuery4 extends Neo4jQuery<LdbcQuery4, LdbcQuery4Result>
{
    /*
    QUERY 4
    
    TODO poorly specified
    TODO what is time interval 1/2?
    TODO Why is this even necessary?
    "The query finds tags that are discussed among ones friends in time interval 1 and not in time interval 2.
    Typically the first interval is a short span and interval 2 is the time from the start of the dataset to the start if the first interval. 
    The query will quite often come out empty, however this depends on the size of the intervals."

    find posts by friends
    filter those posts by date (within the 24hours preceding the input parameter date)
    get tags for those comments
    return those tags and their counts
    
    Find top 10 most popular topics-hashtags (by the number of comments and posts) that your friends have been talking 
    about in last 24 hours (parameter), but not before that.        
    
    PARAMETERS:
            
    Person.Id
    startDate
    Duration
    
    RETURN:
    
    Tag.name
    count
     */
}
