package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery2;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery2EmbeddedCypher implements Neo4jQuery2 {
    private static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"
            + "WHERE post." + Domain.Post.CREATION_DATE + " <= {max_date}\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS personId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS personLastName,"
            + " post." + Domain.Post.ID + " AS postId,"
            + " post." + Domain.Post.CONTENT + " AS postContent,"
            + " post." + Domain.Post.CREATION_DATE + " AS postDate\n"
            + "ORDER BY postDate DESC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery2Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery2 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery2Result>() {
                    @Override
                    public LdbcQuery2Result apply(Map<String, Object> input) {
                        return new LdbcQuery2Result(
                                (long) input.get("personId"),
                                (String) input.get("personFirstName"), (String) input.get("personLastName"),
                                (long) input.get("postId"), (String) input.get("postContent"),
                                (long) input.get("postDate"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery2 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("max_date", operation.maxDateAsMilli());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
