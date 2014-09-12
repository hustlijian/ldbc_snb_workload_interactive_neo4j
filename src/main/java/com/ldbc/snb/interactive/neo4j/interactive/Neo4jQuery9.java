package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.snb.interactive.neo4j.Domain;

public abstract class Neo4jQuery9<CONNECTION> implements Neo4jQuery<LdbcQuery9, LdbcQuery9Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer LATEST_DATE = 2;
    protected static final Integer LIMIT = 3;

    /*
    Given a start Person, find the (most recent) Posts/Comments created by that Person's friends or friends of friends (excluding start Person).
    Only consider the Posts/Comments created before a given date (excluding that date).
    Return the top 20 Posts/Comments, and the Person that created each of those Posts/Comments.
    Sort results descending by creation date of Post/Comment, and then ascending by Post/Comment identifier.
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "*1..2]-(friend:" + Domain.Nodes.Person + ")"
            + "<-[:" + Domain.Rels.HAS_CREATOR + "]-(message)\n"
            + "WHERE message." + Domain.Message.CREATION_DATE + " < {" + LATEST_DATE + "}\n"
            + "RETURN DISTINCT"
            + " message." + Domain.Message.ID + " AS messageId,"
            + " message." + Domain.Message.CONTENT + " AS messageContent,"
            + " message." + Domain.Message.CREATION_DATE + " AS messageCreationDate,"
            + " friend." + Domain.Person.ID + " AS personId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS personLastName\n"
            + "ORDER BY message.creationDate DESC, message.id ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
