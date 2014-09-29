package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.snb.interactive.neo4j.Domain;

public abstract class Neo4jQuery1<CONNECTION> implements Neo4jQuery<LdbcQuery1, LdbcQuery1Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer FRIEND_FIRST_NAME = 2;
    protected static final Integer LIMIT = 3;

    /*
    Given a start Person, find up to 20 Persons with a given first name that the start Person is connected to (excluding start Person) by at most 3 steps via Knows relationships.
    Return Persons, including summaries of the Persons’ workplaces and places of study.
    Sort results ascending by their distance from the start Person, for Persons within the same distance sort by their last name, and for Persons with same last name by their identifier
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[path:" + Domain.Rels.KNOWS + "*1..3]-(friend:" + Domain.Nodes.Person + ")\n"
            + "WHERE friend." + Domain.Person.FIRST_NAME + " = {" + FRIEND_FIRST_NAME + "}\n"
            + "WITH friend, min(length(path)) AS distance\n"
            + "ORDER BY distance ASC, friend." + Domain.Person.LAST_NAME + " ASC, friend." + Domain.Person.ID + " ASC\n"
            + "LIMIT {" + LIMIT + "}\n"
            // TODO OPTIONAL MATCH only necessary if some people don't have a City
//            + "OPTIONAL MATCH (friend)-[:" + Domain.Rels.IS_LOCATED_IN + "]->(friendCity:" + Domain.Place.Type.City + ")\n"
            + "MATCH (friend)-[:" + Domain.Rels.IS_LOCATED_IN + "]->(friendCity:" + Domain.Place.Type.City + ")\n"
            + "OPTIONAL MATCH (friend)-[studyAt:" + Domain.Rels.STUDY_AT + "]->(uni:" + Domain.Organisation.Type.University + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(uniCity:" + Domain.Place.Type.City + ")\n"

            + "WITH friend, collect(CASE uni." + Domain.Organisation.NAME + " WHEN null THEN null ELSE [uni." + Domain.Organisation.NAME + ", studyAt." + Domain.StudiesAt.CLASS_YEAR + ", uniCity." + Domain.Place.NAME + "] END) AS unis, friendCity, distance\n"

            + "OPTIONAL MATCH (friend)-[worksAt:" + Domain.Rels.WORKS_AT + "]->(company:" + Domain.Organisation.Type.Company + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(companyCountry:" + Domain.Place.Type.Country + ")\n"
            + "WITH friend, collect(CASE company." + Domain.Organisation.NAME + " WHEN null THEN null ELSE [company." + Domain.Organisation.NAME + ", worksAt." + Domain.WorksAt.WORK_FROM + ", companyCountry." + Domain.Place.NAME + "] END) AS companies, unis, friendCity, distance\n"

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
}
