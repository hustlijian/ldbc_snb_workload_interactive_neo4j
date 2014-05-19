package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery5Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery5;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery5EmbeddedCypher implements Neo4jQuery5 {
    private static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.KNOWS + "*1..2]-(friend:" + Domain.Nodes.Person + ")<-[membership:" + Domain.Rels.HAS_MEMBER + "]-(forum:" + Domain.Nodes.Forum + ")\n"
            + "WHERE membership." + Domain.HasMember.JOIN_DATE + ">{join_date}\n"
            + "MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")<-[:" + Domain.Rels.CONTAINER_OF + "]-(forum)\n"
            + "RETURN forum." + Domain.Forum.TITLE + " AS forum, count(post) AS postCount\n"
            + "ORDER BY postCount DESC";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery5Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery5 operation) {
        Map<String, Object> cypherParams = buildParams(operation.personId(), operation.joinDate());
        Function<Map<String, Object>, LdbcQuery5Result> transformFun = new Function<Map<String, Object>, LdbcQuery5Result>() {
            @Override
            public LdbcQuery5Result apply(Map<String, Object> row) {
                return new LdbcQuery5Result((String) row.get("forum"), (long) row.get("postCount"));
            }
        };
        return Iterables.transform(engine.execute(QUERY_STRING, cypherParams), transformFun).iterator();
    }

    private Map<String, Object> buildParams(long personId, Date date) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("person_id", personId);
        queryParams.put("join_date", date.getTime());
        return queryParams;
    }

}
