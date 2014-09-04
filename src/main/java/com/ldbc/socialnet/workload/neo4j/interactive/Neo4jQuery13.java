package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery13<CONNECTION> implements Neo4jQuery<LdbcQuery13, LdbcQuery13Result, CONNECTION> {
    protected static final Integer PERSON_ID_1 = 1;
    protected static final Integer PERSON_ID_2 = 2;

    /*
    Given two Persons, find the shortest path between these two Persons in the sub-graph induced by the Knows relationships.
    Return the length of this path.
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (person1:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID_1 + "}}), (person2:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID_2 + "}})\n"
            + "OPTIONAL MATCH path = shortestPath((person1)-[:" + Domain.Rels.KNOWS + "]-(person2))\n"
            + "RETURN CASE path IS NULL WHEN true THEN -1 ELSE length(path) END AS pathLength ";
}
