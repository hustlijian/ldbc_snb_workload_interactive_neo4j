package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery5<CONNECTION> implements Neo4jQuery<LdbcQuery5, LdbcQuery5Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer JOIN_DATE = 2;
    protected static final Integer LIMIT = 3;

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "*1..2]-(friend:" + Domain.Nodes.Person + ")<-[membership:" + Domain.Rels.HAS_MEMBER + "]-(forum:" + Domain.Nodes.Forum + ")\n"
            + "WHERE membership." + Domain.HasMember.JOIN_DATE + ">{" + JOIN_DATE + "}\n"
            + "MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")<-[:" + Domain.Rels.CONTAINER_OF + "]-(forum)\n"
            + "RETURN forum." + Domain.Forum.TITLE + " AS forum, count(post) AS postCount\n"
            + "ORDER BY postCount DESC\n"
            + "LIMIT {" + LIMIT + "}";

}
