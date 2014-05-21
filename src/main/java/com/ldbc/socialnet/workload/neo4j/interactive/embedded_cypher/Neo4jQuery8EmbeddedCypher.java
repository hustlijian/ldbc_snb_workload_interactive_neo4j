package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery8Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery8;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery8EmbeddedCypher extends Neo4jQuery8<ExecutionEngine> {
    private static final String QUERY_STRING = ""
            + "MATCH (:" + Nodes.Person + " {" + Person.ID + ":{person_id}})<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")\n"
            + "MATCH (post)<-[:" + Rels.REPLY_OF + "*]-(comment:" + Nodes.Comment + ")-[:" + Rels.HAS_CREATOR + "]->(person:" + Nodes.Person + ")\n"
            + "RETURN"
            + " person." + Person.ID + " AS personId,"
            + " person." + Person.FIRST_NAME + " AS personFirstName,"
            + " person." + Person.LAST_NAME + " AS personLastName,"
            + " comment." + Comment.ID + " AS commentId,"
            + " comment." + Comment.CREATION_DATE + " AS commentCreationDate,"
            + " comment." + Comment.CONTENT + " AS commentContent\n"
            + "ORDER BY commentCreationDate DESC, commentId ASC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery8Result> execute(ExecutionEngine engine, LdbcQuery8 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery8Result>() {
                    @Override
                    public LdbcQuery8Result apply(Map<String, Object> row) {
                        return new LdbcQuery8Result(
                                (long) row.get("personId"),
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                (long) row.get("commentCreationDate"),
                                (long) row.get("commentId"),
                                (String) row.get("commentContent"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery8 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
