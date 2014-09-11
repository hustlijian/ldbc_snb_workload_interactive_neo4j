package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;

import static com.ldbc.snb.interactive.neo4j.Domain.*;

public abstract class Neo4jQuery6<CONNECTION> implements Neo4jQuery<LdbcQuery6, LdbcQuery6Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer TAG_NAME = 2;
    protected static final Integer LIMIT = 3;

    /*
    Given a start Person and some Tag, find the other Tags that occur together with this Tag on Posts that were created by start Person's friends and friends of friends (excluding start Person).
    Return top 10 Tags, and the count of Posts that were created by these Persons, which contain this Tag.
    Sort results descending by count, and then ascending by Tag name.
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID + "}})-[:" + Rels.KNOWS + "*1..2]-(friend:" + Nodes.Person + ")"
            + "<-[:" + Rels.HAS_CREATOR + "]-(friendPost:" + Nodes.Post + ")-[:" + Rels.HAS_TAG + "]->(knownTag:" + Nodes.Tag + " {" + Tag.NAME + ":{" + TAG_NAME + "}})\n"
            + "WHERE not(person=friend)\n"
            + "MATCH (friendPost)-[:" + Rels.HAS_TAG + "]->(commonTag:" + Nodes.Tag + ")\n"
            + "WHERE not(commonTag=knownTag)\n"
            + "WITH DISTINCT commonTag, knownTag, friend\n"
            + "MATCH (commonTag)<-[:" + Rels.HAS_TAG + "]-(commonPost:" + Nodes.Post + ")-[:" + Rels.HAS_TAG + "]->(knownTag)\n"
            + "WHERE (commonPost)-[:" + Rels.HAS_CREATOR + "]->(friend)\n"
            + "RETURN commonTag." + Tag.NAME + " AS tagName, count(commonPost) AS postCount\n"
            + "ORDER BY postCount DESC, tagName ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
