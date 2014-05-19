package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery4;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery4EmbeddedCypher implements Neo4jQuery4 {
    private static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")"
            + "-[" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")\n"
            + "WHERE post." + Domain.Post.CREATION_DATE + " >= {min_date} AND post." + Domain.Post.CREATION_DATE + " <= {max_date}\n"
            + "WITH DISTINCT tag, collect(tag) AS tags\n"
            + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(tags) AS tagCount\n"
            + "ORDER BY tagCount DESC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery4Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery4 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery4Result>() {
                    @Override
                    public LdbcQuery4Result apply(Map<String, Object> input) {
                        return new LdbcQuery4Result((String) input.get("tagName"), (int) input.get("tagCount"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery4 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("min_date", operation.minDateAsMilli());
        queryParams.put("max_date", operation.maxDateAsMilli());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
