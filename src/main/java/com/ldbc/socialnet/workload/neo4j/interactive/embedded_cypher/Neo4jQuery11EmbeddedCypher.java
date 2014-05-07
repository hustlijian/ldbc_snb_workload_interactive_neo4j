package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery11Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery11;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery11EmbeddedCypher implements Neo4jQuery11 {
    /*
    Q11 - Referral
    Description
        Find a friend of the specified person, or a friend of his friend (excluding the specified person), who has long worked in a company in a specified country.
        Sort ascending by start date, and then ascending by person URI. Top 10 should be shown.
    Parameter
        Person
        Country
        max workFrom date
    Result (for each result return)
        Person.firstName
        Person.lastName
        Person-worksAt->.worksFrom
        Person-worksAt->Organization.name
        Person.Id
    */

    private static final String QUERY_STRING = ""
            + "MATCH (:" + Nodes.Person + " {" + Person.ID + ":{person_id}})-[:" + Rels.KNOWS + "*1..2]-(friend:" + Nodes.Person + ")\n"
            + "WITH DISTINCT friend\n"
            + "MATCH (friend)-[worksAt:" + Rels.WORKS_AT + "]->(company:" + Organisation.Type.Company + ")\n"
            + "WHERE worksAt." + WorksAt.WORK_FROM + " <= {work_from_year} AND "
            + " (company)-[:" + Rels.IS_LOCATED_IN + "]->(:" + Place.Type.Country + " {" + Place.NAME + ":{country_name}})\n"
            + "RETURN"
            + " friend." + Person.ID + " AS friendId,"
            + " friend." + Person.FIRST_NAME + " AS friendFirstName,"
            + " friend." + Person.LAST_NAME + " AS friendLastName,"
            + " worksAt." + WorksAt.WORK_FROM + " AS workFromYear,"
            + " company." + Organisation.NAME + " AS companyName\n"
            + "ORDER BY workFromYear ASC, friendId ASC\n"
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery11Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery11 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery11Result>() {
                    @Override
                    public LdbcQuery11Result apply(Map<String, Object> row) {
                        System.out.println(MapUtils.prettyPrint(row));
                        return new LdbcQuery11Result(
                                (long) row.get("friendId"),
                                (String) row.get("friendFirstName"),
                                (String) row.get("friendLastName"),
                                (String) row.get("companyName"),
                                (int) row.get("workFromYear"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery11 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("country_name", operation.country());
        queryParams.put("work_from_year", operation.workFromYear());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
