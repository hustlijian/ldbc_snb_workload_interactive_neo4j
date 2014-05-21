package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery11Result;

public abstract class Neo4jQuery11<CONNECTION> implements Neo4jQuery<LdbcQuery11, LdbcQuery11Result, CONNECTION> {
    /*
    Q11 - Referral
    Description
        Find a friend of the specified person, or a friend of his friend (excluding the specified person), who has long worked in a company in a specified country.
        Sort ascending by start date, and then ascending by person URI. Top 10 should be shown.
    Parameter
        Person
        Country
        limit date
    Result (for each result return)
        Person.firstName
        Person.lastName
        Person-worksAt->.worksFrom
        Person-worksAt->Organization.name
        Person.Id
    */
}
