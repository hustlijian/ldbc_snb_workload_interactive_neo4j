package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery12<CONNECTION> implements Neo4jQuery<LdbcQuery12, LdbcQuery12Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer TAG_CLASS_ID = 2;
    protected static final Integer LIMIT = 3;

    // TODO GOOD AND FINAL
    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")\n"
            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(comment:" + Domain.Nodes.Comment + ")-[:" + Domain.Rels.REPLY_OF + "*]->()-[:" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")"
            + "-[:" + Domain.Rels.HAS_TYPE + "]->(tagClass:" + Domain.Nodes.TagClass + ")-[:" + Domain.Rels.IS_SUBCLASS_OF + "*0..]->(baseTagClass:" + Domain.Nodes.TagClass + ")\n"
            + "WHERE tagClass." + Domain.TagClass.URI + " = {" + TAG_CLASS_ID + "} OR baseTagClass." + Domain.TagClass.URI + " = {" + TAG_CLASS_ID + "}\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS friendId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS friendFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS friendLastName,"
            + " collect(DISTINCT tag." + Domain.Tag.NAME + ") AS tagNames,"
            + " count(DISTINCT comment) AS count\n"
            + "ORDER BY count DESC, friendId ASC\n"
            + "LIMIT {" + LIMIT + "}";

    // TODO NOT GOOD BUT FINAL
//    private static final String QUERY_STRING = ""
//            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")\n"
//            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(comment:" + Domain.Nodes.Comment + ")-[:" + Domain.Rels.REPLY_OF + "*]->()-[:" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")-[:" + Domain.Rels.HAS_TYPE + "]->(tagClass:" + Domain.Nodes.TagClass + ")\n"
//            + "WHERE tagClass." + Domain.TagClass.URI + " = {tag_class_id} OR (tagClass)-[:" + Domain.Rels.IS_SUBCLASS_OF + "*0..]->(:" + Domain.Nodes.TagClass + " {" + Domain.TagClass.URI + ":{tag_class_id}})\n"
//            + "RETURN"
//            + " friend." + Domain.Person.ID + " AS friendId,"
//            + " friend." + Domain.Person.FIRST_NAME + " AS friendFirstName,"
//            + " friend." + Domain.Person.LAST_NAME + " AS friendLastName,"
//            + " collect(DISTINCT tag." + Domain.Tag.NAME + ") AS tagNames,"
//            + " count(DISTINCT comment) AS count\n"
//            + "ORDER BY count DESC, friendId ASC\n"
//            + "LIMIT {limit}";

    // TODO GOOD AND FINAL - with more specific OPTIONAL MATCH section
//    private static final String QUERY_STRING = ""
//            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")\n"
//            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(comment:" + Domain.Nodes.Comment + ")-[:" + Domain.Rels.REPLY_OF + "*0..]->(:" + Domain.Nodes.Comment + ")-[:" + Domain.Rels.REPLY_OF + "]->(:" + Domain.Nodes.Post + ")-[:" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")-[:" + Domain.Rels.HAS_TYPE + "]->(tagClass:" + Domain.Nodes.TagClass + ")-[:" + Domain.Rels.IS_SUBCLASS_OF + "*0..]->(baseTagClass:" + Domain.Nodes.TagClass + ")\n"
//            + "WHERE tagClass." + Domain.TagClass.URI + " = {tag_class_id} OR baseTagClass." + Domain.TagClass.URI + " = {tag_class_id}\n"
//            + "RETURN"
//            + " friend." + Domain.Person.ID + " AS friendId,"
//            + " friend." + Domain.Person.FIRST_NAME + " AS friendFirstName,"
//            + " friend." + Domain.Person.LAST_NAME + " AS friendLastName,"
//            + " collect(DISTINCT tag." + Domain.Tag.NAME + ") AS tagNames,"
//            + " count(DISTINCT comment) AS count\n"
//            + "ORDER BY count DESC, friendId ASC\n"
//            + "LIMIT {limit}";
}
