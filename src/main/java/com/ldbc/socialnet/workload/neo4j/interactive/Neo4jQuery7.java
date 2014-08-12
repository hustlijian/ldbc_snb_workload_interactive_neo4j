package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery7<CONNECTION> implements Neo4jQuery<LdbcQuery7, LdbcQuery7Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer LIMIT = 2;

    protected static final String QUERY_STRING = ""
            + "MATCH (start:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")"
            + "<-[like:" + Domain.Rels.LIKES + "]-(person:" + Domain.Nodes.Person + ")\n"
            + "RETURN person." + Domain.Person.ID + " AS personId, person." + Domain.Person.FIRST_NAME + " AS personFirstName, person." + Domain.Person.LAST_NAME + " AS personLastName,"
            + " like." + Domain.Likes.CREATION_DATE + " AS likeDate,  NOT((person)-[:" + Domain.Rels.KNOWS + "]-(start)) AS isNew, post." + Domain.Post.ID + " AS postId,"
            + " post." + Domain.Post.CONTENT + " AS postContent, like." + Domain.Likes.CREATION_DATE + " - post." + Domain.Post.CREATION_DATE + " AS latency\n"
            + "ORDER BY like." + Domain.Likes.CREATION_DATE + " DESC, personId ASC\n"
            + "LIMIT {" + LIMIT + "}";

}
