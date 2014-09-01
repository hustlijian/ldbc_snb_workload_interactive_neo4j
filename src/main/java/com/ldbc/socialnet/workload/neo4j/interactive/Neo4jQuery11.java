package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery11<CONNECTION> implements Neo4jQuery<LdbcQuery11, LdbcQuery11Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer WORK_FROM_YEAR = 2;
    protected static final Integer COUNTRY_NAME = 3;
    protected static final Integer LIMIT = 4;

    /*
    Given a start Person, find that Person's friends and friends of friends (excluding start Person) who started Working in some Company in a given Country, before a given date (year).
    Return top 10 Persons, the Company they worked at, and the year they started working at that Company.
    Sort results ascending by the start date, then ascending by Person identifier, and lastly by Organization name
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "*1..2]-(friend:" + Domain.Nodes.Person + ")\n"
            + "WHERE not(person=friend)\n"
            + "WITH DISTINCT friend\n"
            + "MATCH (friend)-[worksAt:" + Domain.Rels.WORKS_AT + "]->(company:" + Domain.Organisation.Type.Company + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(:" + Domain.Place.Type.Country + " {" + Domain.Place.NAME + ":{" + COUNTRY_NAME + "}})\n"
            + "WHERE worksAt." + Domain.WorksAt.WORK_FROM + " < {" + WORK_FROM_YEAR + "}\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS friendId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS friendFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS friendLastName,"
            + " worksAt." + Domain.WorksAt.WORK_FROM + " AS workFromYear,"
            + " company." + Domain.Organisation.NAME + " AS companyName\n"
            + "ORDER BY workFromYear ASC, friendId ASC, companyName DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
