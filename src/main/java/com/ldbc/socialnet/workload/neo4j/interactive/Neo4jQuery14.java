package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result;

public abstract class Neo4jQuery14<CONNECTION> implements Neo4jQuery<LdbcQuery14, LdbcQuery14Result, CONNECTION> {
/*
    Description
        What is the best way of approaching a specific person that is not a direct contact?
        We are looking for an introduction via the people most connected to the target person.
        The strength of connection is taken to be proportional to the number of messages exchanged between the connected persons.
        The query returns paths, and their weights. A path is an array of three element records giving start vertex, end vertex and weight.
        The weight is computed by adding up the message flow between the end points. Reply to a post counts as 1, reply to a comment counts as 0.5.
        The weight is symmetrical. The result should be sorted descending by weight, and only top 10 paths should be returned.
    Parameter
        Person1.id
        Person2.id
    Result (for each result return)
        the whole path between two persons
        weight of the path


    Description
        Find all paths between two specified persons, where paths may be comprised of: Person, Comment, and Post entities; Knows, ReplyOf, and HasCreator relationships.
        Calculate the weight of the paths given the following rules: each reply to a post contributes 1.0 to the weight, each reply to a comment contributes 0.5 to the weight.
        The weight should be symmetrical.
        The result should be sorted descending by weight, and only top 10 paths should be returned.
    Parameter
        Person1.id
        Person2.id
    Result
        path - ordered sequence of IDs, alternating between entity IDs and relationship IDs, and starting & ending with the IDs of the connected persons
        weight
*/
}
