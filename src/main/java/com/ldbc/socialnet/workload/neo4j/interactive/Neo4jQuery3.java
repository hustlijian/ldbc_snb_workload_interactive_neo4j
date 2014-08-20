package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public abstract class Neo4jQuery3<CONNECTION> implements Neo4jQuery<LdbcQuery3, LdbcQuery3Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer COUNTRY_X = 2;
    protected static final Integer COUNTRY_Y = 3;
    protected static final Integer MIN_DATE = 4;
    protected static final Integer MAX_DATE = 5;
    protected static final Integer LIMIT = 6;

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID + "}})-[:" + Rels.KNOWS + "*1..2]-(friend:" + Nodes.Person + ")"
            + "<-[:" + Rels.HAS_CREATOR + "]-(postX:" + Nodes.Post + ")-[:" + Rels.IS_LOCATED_IN + "]->(countryX:" + Place.Type.Country + ")\n"
            + "WHERE countryX." + Place.NAME + "={" + COUNTRY_X + "} AND postX." + Message.CREATION_DATE + ">={" + MIN_DATE + "} AND postX." + Message.CREATION_DATE + "<={" + MAX_DATE + "}\n"
            + "WITH friend, count(DISTINCT postX) AS xCount\n"
            + "MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(postY:" + Nodes.Post + ")-[:" + Rels.IS_LOCATED_IN + "]->(countryY:" + Place.Type.Country + " {" + Place.NAME + ":{" + COUNTRY_Y + "}})\n"
            + "WHERE postY." + Message.CREATION_DATE + ">={" + MIN_DATE + "} AND postY." + Message.CREATION_DATE + "<={" + MAX_DATE + "}\n"
            + "WITH friend." + Person.ID + " AS friendId, friend." + Person.FIRST_NAME + " AS friendFirstName, friend." + Person.LAST_NAME + " AS friendLastName , xCount, count(DISTINCT postY) AS yCount\n"
            + "RETURN friendId, friendFirstName, friendLastName, xCount, yCount, xCount + yCount AS xyCount\n"
            + "ORDER BY xyCount DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
