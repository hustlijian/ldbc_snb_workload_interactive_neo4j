package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery3;
import org.neo4j.cypher.javacompat.ExecutionEngine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Neo4jQuery3EmbeddedCypher extends Neo4jQuery3<ExecutionEngine> {
    private static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.KNOWS + "*1..2]-(friend:" + Domain.Nodes.Person + ")"
            + "<-[:" + Domain.Rels.HAS_CREATOR + "]-(postX:" + Domain.Nodes.Post + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(countryX:" + Domain.Place.Type.Country + ")\n"
            + "WHERE countryX." + Domain.Place.NAME + "={country_x} AND postX." + Domain.Post.CREATION_DATE + ">={min_date} AND postX." + Domain.Post.CREATION_DATE + "<={max_date}\n"
            + "WITH friend, count(DISTINCT postX) AS xCount\n"
            + "MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(postY:" + Domain.Nodes.Post + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(countryY:" + Domain.Place.Type.Country + " {" + Domain.Place.NAME + ":{country_y}})\n"
            + "WHERE postY." + Domain.Post.CREATION_DATE + ">={min_date} AND postY." + Domain.Post.CREATION_DATE + "<={max_date}\n"
            + "WITH friend." + Domain.Person.FIRST_NAME + " + ' ' + friend." + Domain.Person.LAST_NAME + " AS friendName , xCount, count(DISTINCT postY) AS yCount\n"
            + "RETURN friendName, xCount, yCount, xCount + yCount AS xyCount\n"
            + "ORDER BY xyCount DESC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery3Result> execute(ExecutionEngine engine, LdbcQuery3 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery3Result>() {
                    @Override
                    public LdbcQuery3Result apply(Map<String, Object> input) {
                        return new LdbcQuery3Result((String) input.get("friendName"), (long) input.get("xCount"),
                                (long) input.get("yCount"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery3 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("country_x", operation.countryX());
        queryParams.put("country_y", operation.countryY());
        queryParams.put("min_date", operation.startDateAsMilli());
        queryParams.put("max_date", operation.endDateAsMilli());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
