package com.ldbc.socialnet.workload.neo4j.transaction;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3Result;

public interface Neo4jQuery3 extends Neo4jQuery<LdbcQuery3, LdbcQuery3Result>
{
    /*
    QUERY 3 
    
    Find Friends and Friends of Friends of the user A that have been to the countries X and Y within a specified period.
     
    PARAMETERS:
     
    Person.Id
    CountryX.Name
    CountryY.Name
    startDate - the beginning of the requested period (the latest date)
    Duration - the duration of the requested period
     
    RETURN:
    
    Person.Id
    ct1 = the number of post from the first country
    ct2 = the number of post from the second country
    ct = ct1 + ct2
     */
}
