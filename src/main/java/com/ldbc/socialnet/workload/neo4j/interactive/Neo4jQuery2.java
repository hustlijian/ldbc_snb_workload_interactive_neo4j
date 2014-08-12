package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery2<CONNECTION> implements Neo4jQuery<LdbcQuery2, LdbcQuery2Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer MAX_DATE = 2;
    protected static final Integer LIMIT = 3;

    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"
            + "WHERE post." + Domain.Post.CREATION_DATE + " <= {" + MAX_DATE + "}\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS personId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS personLastName,"
            + " post." + Domain.Post.ID + " AS postId,"
            + " post." + Domain.Post.CONTENT + " AS postContent,"
            + " post." + Domain.Post.CREATION_DATE + " AS postDate\n"
            + "ORDER BY postDate DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
