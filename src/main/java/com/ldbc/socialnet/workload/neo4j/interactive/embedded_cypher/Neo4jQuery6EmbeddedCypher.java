package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery6;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery6EmbeddedCypher extends Neo4jQuery6<ExecutionEngine> {
    private static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{person_id}})-[:" + Rels.KNOWS + "*1..2]-(:" + Nodes.Person + ")<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")-[:" + Rels.HAS_TAG + "]->(:" + Nodes.Tag + " {" + Tag.NAME + ":{tag_name}})\n"
            + "WITH DISTINCT post\n"
            + "MATCH (post)-[:" + Rels.HAS_TAG + "]->(tag:" + Nodes.Tag + ")\n"
            + "WHERE NOT(tag." + Tag.NAME + "={tag_name})\n"
            + "RETURN tag." + Tag.NAME + " AS tagName, count(tag) AS tagCount\n"
            + "ORDER BY tagCount DESC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery6Result> execute(ExecutionEngine engine, LdbcQuery6 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery6Result>() {
                    @Override
                    public LdbcQuery6Result apply(Map<String, Object> next) {
                        return new LdbcQuery6Result((String) next.get("tagName"), (long) next.get("tagCount"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery6 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("tag_name", operation.tagName());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
