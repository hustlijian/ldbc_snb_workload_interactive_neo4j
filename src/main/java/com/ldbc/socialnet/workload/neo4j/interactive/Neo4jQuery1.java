package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery1<CONNECTION> implements Neo4jQuery<LdbcQuery1, LdbcQuery1Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer FRIEND_FIRST_NAME = 2;
    protected static final Integer LIMIT = 3;

    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[path:" + Domain.Rels.KNOWS + "*]-(friend:" + Domain.Nodes.Person + ")\n"
            + "WHERE friend." + Domain.Person.FIRST_NAME + " = {" + FRIEND_FIRST_NAME + "}\n"
            + "WITH friend, min(length(path)) AS distance\n"
            + "ORDER BY distance ASC, friend.lastName ASC\n"
            + "LIMIT {" + LIMIT + "}\n"
            // TODO MATCH if all Persons have a City
            + "OPTIONAL MATCH (friend)-[:" + Domain.Rels.IS_LOCATED_IN + "]->(friendCity:" + Domain.Place.Type.City + ")\n"

            + "OPTIONAL MATCH (friend)-[studyAt:" + Domain.Rels.STUDY_AT + "]->(uni:" + Domain.Organisation.Type.University + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(uniCity:" + Domain.Place.Type.City + ")\n"
            + "WITH friend, collect(uni." + Domain.Organisation.NAME + " + ',' + uniCity." + Domain.Place.NAME + " + ',' + studyAt." + Domain.StudiesAt.CLASS_YEAR + ") AS unis, friendCity, distance\n"

            + "OPTIONAL MATCH (friend)-[worksAt:" + Domain.Rels.WORKS_AT + "]->(company:" + Domain.Organisation.Type.Company + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(companyCountry:" + Domain.Nodes.Place + ":" + Domain.Place.Type.Country + ")\n"
            + "WITH friend, collect(company." + Domain.Organisation.NAME + " + ',' + companyCountry." + Domain.Place.NAME + " + ',' + worksAt." + Domain.WorksAt.WORK_FROM + ") AS companies, unis, friendCity, distance\n"

            + "RETURN"
            + " friend." + Domain.Person.ID + " AS id,"
            + " friend." + Domain.Person.LAST_NAME + " AS lastName,"
            + " distance,"
            + " friend." + Domain.Person.BIRTHDAY + " AS birthday,"
            + " friend." + Domain.Person.CREATION_DATE + " AS creationDate,"
            + " friend." + Domain.Person.GENDER + " AS gender,"
            + " friend." + Domain.Person.BROWSER_USED + " AS browser,"
            + " friend." + Domain.Person.LOCATION_IP + " AS locationIp,"
            + " friend." + Domain.Person.EMAIL_ADDRESSES + " AS emails,"
            + " friend." + Domain.Person.LANGUAGES + " AS languages,"
            + " friendCity." + Domain.Place.NAME + " AS cityName,"
            + " unis,"
            + " companies\n"
            + "ORDER BY distance ASC, friend." + Domain.Person.LAST_NAME + " ASC, friend." + Domain.Person.ID + " ASC\n"
            + "LIMIT {" + LIMIT + "}";

    // TODO try this shortest path approach too
    /*
        MATCH path = shortestPath((:Person {id:{person_id}})-[:KNOWS*]-(friend:Person {firstName:{friend_first_name}}))
        RETURN friend, min(length(path)) AS distance
        ORDER BY distance ASC, friend.lastName ASC
        LIMIT {limit}
     */
}
