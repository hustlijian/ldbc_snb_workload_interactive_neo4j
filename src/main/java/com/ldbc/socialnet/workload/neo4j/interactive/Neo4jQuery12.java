package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12Result;

public abstract class Neo4jQuery12<CONNECTION> implements Neo4jQuery<LdbcQuery12, LdbcQuery12Result, CONNECTION> {
/*
    Description
        Find friends of a specified user that have replied the most to posts with a tag in a given category.
        The result should be sorted descending by number of replies, and then ascending by friend's URI. Top 20 should be shown.
    Parameter
        Person
        Tag category
    Result (for each result return)
        Person.id
        Person.firstName
        Person.lastName
        Tag.name
        count
 */
}
