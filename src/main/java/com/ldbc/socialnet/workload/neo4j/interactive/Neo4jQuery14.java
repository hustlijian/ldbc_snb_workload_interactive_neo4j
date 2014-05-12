package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result;

public interface Neo4jQuery14 extends Neo4jQuery<LdbcQuery14, LdbcQuery14Result> {
/*
    Description
        What is the best way of approaching a specific person that is not a direct contact?
        We are looking for an introduction via the people most connected to the target person.
        The strength of connection is taken to be proportional to the number of messages exchanged between the connected persons.
        The query returns paths, and their weights. A path is an array of three element records giving start vertex, end vertex and weight.
        The weight is computed by adding up the message flow between the end points. Reply to a post counts as 1, reply to a comment counts as 0.5.
        The weight is symmetrical. The result should be sorted descending by weight, and only top 10 paths should be returned.
    Parameter
        Person1
        Person2
    Result (for each result return)
        the whole path between two persons
        weight of the path
*/
}
