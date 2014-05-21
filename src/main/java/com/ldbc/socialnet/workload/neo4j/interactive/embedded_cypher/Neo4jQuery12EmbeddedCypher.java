package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery12Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery12;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery12EmbeddedCypher extends Neo4jQuery12<ExecutionEngine> {
    /*
    Q12 - Expert Search
    Description
        Find friends of a specified user that have replied the most to posts with a tag in a given category.
        The result should be sorted descending by number of replies, and then ascending by friend's URI. Top 20 should be shown.
    Parameter
        Person.id
        TagClass.id
    Result (for each result return)
        Person.id
        Person.firstName
        Person.lastName
        Tag.name
        count
    */

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

    //    // TODO GOOD AND FINAL
    private static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")\n"
            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(comment:" + Domain.Nodes.Comment + ")-[:" + Domain.Rels.REPLY_OF + "*]->()-[:" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")-[:" + Domain.Rels.HAS_TYPE + "]->(tagClass:" + Domain.Nodes.TagClass + ")-[:" + Domain.Rels.IS_SUBCLASS_OF + "*0..]->(baseTagClass:" + Domain.Nodes.TagClass + ")\n"
            + "WHERE tagClass." + Domain.TagClass.URI + " = {tag_class_id} OR baseTagClass." + Domain.TagClass.URI + " = {tag_class_id}\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS friendId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS friendFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS friendLastName,"
            + " collect(DISTINCT tag." + Domain.Tag.NAME + ") AS tagNames,"
            + " count(DISTINCT comment) AS count\n"
            + "ORDER BY count DESC, friendId ASC\n"
            + "LIMIT {limit}";

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

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery12Result> execute(ExecutionEngine engine, LdbcQuery12 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery12Result>() {
                    @Override
                    public LdbcQuery12Result apply(Map<String, Object> row) {
                        return new LdbcQuery12Result(
                                (long) row.get("friendId"),
                                (String) row.get("friendFirstName"),
                                (String) row.get("friendLastName"),
                                (Collection<String>) row.get("tagNames"),
                                (long) row.get("count"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery12 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("tag_class_id", operation.tagClassId());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
