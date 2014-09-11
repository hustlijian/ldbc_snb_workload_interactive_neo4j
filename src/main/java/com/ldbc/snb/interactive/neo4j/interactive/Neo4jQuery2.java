package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.snb.interactive.neo4j.Domain;

public abstract class Neo4jQuery2<CONNECTION> implements Neo4jQuery<LdbcQuery2, LdbcQuery2Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer MAX_DATE = 2;
    protected static final Integer LIMIT = 3;

    /*
    Given a start Person, find (most recent) Posts and Comments from all of that Person's friends, that were created before (and including) a given date.
    Return the top 20 Posts/Comments, and the Person that created each of them.
    Sort results descending by creation date, and then ascending by Post identifier.
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")<-[:" + Domain.Rels.HAS_CREATOR + "]-(message)\n"
            + "WHERE message." + Domain.Message.CREATION_DATE + " <= {" + MAX_DATE + "} AND (message:" + Domain.Nodes.Post + " OR message:" + Domain.Nodes.Comment + ")\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS personId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS personLastName,"
            + " message." + Domain.Message.ID + " AS messageId,"
            + " message." + Domain.Message.CONTENT + " AS messageContent,"
            + " message." + Domain.Message.CREATION_DATE + " AS messageDate\n"
            + "ORDER BY messageDate DESC, messageId ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
