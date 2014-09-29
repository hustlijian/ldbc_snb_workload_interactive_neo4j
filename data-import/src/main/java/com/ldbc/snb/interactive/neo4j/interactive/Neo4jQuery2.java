package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;

import static com.ldbc.snb.interactive.neo4j.Domain.*;

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
            + "MATCH (:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID + "}})-[:" + Rels.KNOWS + "]-(friend:" + Nodes.Person + ")<-[:" + Rels.HAS_CREATOR + "]-(message)\n"
            + "WHERE message." + Message.CREATION_DATE + " <= {" + MAX_DATE + "} AND (message:" + Nodes.Post + " OR message:" + Nodes.Comment + ")\n"
            + "RETURN"
            + " friend." + Person.ID + " AS personId,"
            + " friend." + Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Person.LAST_NAME + " AS personLastName,"
            + " message." + Message.ID + " AS messageId,"
            + " CASE has(message." + Message.CONTENT + ") WHEN true THEN message." + Message.CONTENT + " ELSE message." + Post.IMAGE_FILE + " END AS messageContent,\n"
            + " message." + Message.CREATION_DATE + " AS messageDate\n"
            + "ORDER BY messageDate DESC, messageId ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
