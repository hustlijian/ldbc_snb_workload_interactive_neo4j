package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery8<CONNECTION> implements Neo4jQuery<LdbcQuery8, LdbcQuery8Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer LIMIT = 2;

    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"
            + "MATCH (post)<-[:" + Domain.Rels.REPLY_OF + "*]-(comment:" + Domain.Nodes.Comment + ")-[:" + Domain.Rels.HAS_CREATOR + "]->(person:" + Domain.Nodes.Person + ")\n"
            + "RETURN"
            + " person." + Domain.Person.ID + " AS personId,"
            + " person." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " person." + Domain.Person.LAST_NAME + " AS personLastName,"
            + " comment." + Domain.Message.ID + " AS commentId,"
            + " comment." + Domain.Message.CREATION_DATE + " AS commentCreationDate,"
            + " comment." + Domain.Message.CONTENT + " AS commentContent\n"
            + "ORDER BY commentCreationDate DESC, commentId ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
