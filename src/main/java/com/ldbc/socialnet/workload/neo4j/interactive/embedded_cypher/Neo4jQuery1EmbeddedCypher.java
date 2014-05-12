package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery1;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery1EmbeddedCypher implements Neo4jQuery1 {
    /*
    Description
        Given a person, return up to 20 people with a certain first name sort by distance from a given person and for people within the same distance sort by last name.
        Persons are returned (e.g. as for a search page with top 20 shown), and the information is complemented with summaries of the persons’ workplaces and places of study.
    Parameter
        firstName
        Person
    Result (for each result return)
        Person.id
        Person.lastName
        distance from the person
        Person.birthday
        Person.creationDate
        Person.gender
        Person.browserUsed
        Person.locationIP
        (Person.email)
        (Person.language)
        Person-isLocatedIn->Location.name
        (Person-studyAt->University.name,
         Person-studyAt->.classYear,
         Person-studyAt->University-isLocatedIn->City.name)
        (Person-workAt->Company.name,
         Person-workAt->.workFrom,
         Person-workAt->Company-isLocatedIn->City.name)
     */

    private static final String QUERY_STRING = ""
            + "MATCH (:" + Nodes.Person + " {" + Person.ID + ":{person_id}})-[path:" + Rels.KNOWS + "*]-(friend:" + Nodes.Person + ")\n"
            + "WHERE friend." + Person.FIRST_NAME + " = {friend_first_name}\n"
            + "WITH friend, min(length(path)) AS distance\n"
            + "ORDER BY distance ASC, friend.lastName ASC\n"
            + "LIMIT {limit}\n"
            // TODO MATCH if all Persons have a City
            + "OPTIONAL MATCH (friend)-[:" + Rels.IS_LOCATED_IN + "]->(friendCity:" + Place.Type.City + ")\n"

            + "OPTIONAL MATCH (friend)-[studyAt:" + Rels.STUDY_AT + "]->(uni:" + Organisation.Type.University + ")-[:" + Rels.IS_LOCATED_IN + "]->(uniCity:" + Place.Type.City + ")\n"
            + "WITH friend, collect(uni." + Organisation.NAME + " + ',' + uniCity." + Place.NAME + " + ',' + studyAt." + StudiesAt.CLASS_YEAR + ") AS unis, friendCity, distance\n"

            + "OPTIONAL MATCH (friend)-[worksAt:" + Rels.WORKS_AT + "]->(company:" + Organisation.Type.Company + ")-[:" + Rels.IS_LOCATED_IN + "]->(companyCountry:" + Nodes.Place + ":" + Place.Type.Country + ")\n"
            + "WITH friend, collect(company." + Organisation.NAME + " + ',' + companyCountry." + Place.NAME + " + ',' + worksAt." + WorksAt.WORK_FROM + ") AS companies, unis, friendCity, distance\n"

            + "RETURN"
            + " friend." + Person.ID + " AS id,"
            + " friend." + Person.LAST_NAME + " AS lastName,"
            + " distance,"
            + " friend." + Person.BIRTHDAY + " AS birthday,"
            + " friend." + Person.CREATION_DATE + " AS creationDate,"
            + " friend." + Person.GENDER + " AS gender,"
            + " friend." + Person.BROWSER_USED + " AS browser,"
            + " friend." + Person.LOCATION_IP + " AS locationIp,"
            + " friend." + Person.EMAIL_ADDRESSES + " AS emails,"
            + " friend." + Person.LANGUAGES + " AS languages,"
            + " friendCity." + Place.NAME + " AS cityName,"
            + " unis,"
            + " companies\n"
            + "ORDER BY distance ASC, friend." + Person.LAST_NAME + " ASC";

    // TODO try this shortest path approach too
    /*
        MATCH path = shortestPath((:Person {id:{person_id}})-[:KNOWS*]-(friend:Person {firstName:{friend_first_name}}))
        RETURN friend, min(length(path)) AS distance
        ORDER BY distance ASC, friend.lastName ASC
        LIMIT {limit}
     */

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery1Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery1 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery1Result>() {
                    @Override
                    public LdbcQuery1Result apply(Map<String, Object> row) {
                        System.out.println(MapUtils.prettyPrint(row));
                        return new LdbcQuery1Result(
                                (long) row.get("id"),
                                (String) row.get("lastName"),
                                (int) row.get("distance"),
                                (long) row.get("birthday"),
                                (long) row.get("creationDate"),
                                (String) row.get("gender"),
                                (String) row.get("browser"),
                                (String) row.get("locationIp"),
                                Lists.newArrayList((String[]) row.get("emails")),
                                Lists.newArrayList((String[]) row.get("languages")),
                                (String) row.get("cityName"),
                                (Collection<String>) row.get("unis"),
                                (Collection<String>) row.get("companies"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery1 operation) {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("friend_first_name", operation.firstName());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
