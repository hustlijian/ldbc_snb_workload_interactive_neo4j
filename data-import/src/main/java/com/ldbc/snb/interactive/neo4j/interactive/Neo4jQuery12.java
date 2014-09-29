package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.snb.interactive.neo4j.Domain;

public abstract class Neo4jQuery12<CONNECTION> implements Neo4jQuery<LdbcQuery12, LdbcQuery12Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer TAG_CLASS_NAME = 2;
    protected static final Integer LIMIT = 3;

    /*
    Given a start Person, find the Comments that this Person's friends made in reply to Posts,
    considering only those Comments that are immediate (1-hop) replies, not the transitive (multi-hop) case.
    Only consider Posts with a Tag in a given TagClass or in a descendent of that TagClass.
    Count the number of these reply Comments, and collect the Tags that were attached to the Posts they replied to.
    Return top 20 Persons, the reply count, and the collection of Tags.
    Sort results descending by Comment count, and then ascending by Person identifier
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")\n"
            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(comment:" + Domain.Nodes.Comment + ")-[:" + Domain.Rels.REPLY_OF + "]->(:" + Domain.Nodes.Post + ")-[:" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")"
            + "-[:" + Domain.Rels.HAS_TYPE + "]->(tagClass:" + Domain.Nodes.TagClass + ")-[:" + Domain.Rels.IS_SUBCLASS_OF + "*0..]->(baseTagClass:" + Domain.Nodes.TagClass + ")\n"
            + "WHERE tagClass." + Domain.TagClass.NAME + " = {" + TAG_CLASS_NAME + "} OR baseTagClass." + Domain.TagClass.NAME + " = {" + TAG_CLASS_NAME + "}\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS friendId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS friendFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS friendLastName,"
            + " collect(DISTINCT tag." + Domain.Tag.NAME + ") AS tagNames,"
            + " count(DISTINCT comment) AS count\n"
            + "ORDER BY count DESC, friendId ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
