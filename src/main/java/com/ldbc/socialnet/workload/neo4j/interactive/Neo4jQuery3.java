package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public abstract class Neo4jQuery3<CONNECTION> implements Neo4jQuery<LdbcQuery3, LdbcQuery3Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer COUNTRY_X = 2;
    protected static final Integer COUNTRY_Y = 3;
    protected static final Integer MIN_DATE = 4;
    protected static final Integer MAX_DATE = 5;
    protected static final Integer LIMIT = 6;

    /*
Given a start Person, find Persons that are their friends and friends of friends (excluding start Person),
that have made Posts/Comments in the given Countries X and Y within a given period.
Only Persons that are foreign to Countries X and Y are considered, that is Persons whose Location is not Country X or Country Y.
Return top 20 Persons, and their Post/Comment counts.
Sort results descending by total number of Posts/Comments, and then ascending by Person identifier.
     */

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID + "}})-[:" + Rels.KNOWS + "*1..2]-(friend:" + Nodes.Person + ")"
            + "<-[:" + Rels.HAS_CREATOR + "]-(messageX)-[:" + Rels.IS_LOCATED_IN + "]->(countryX:" + Place.Type.Country + ")\n"
            + "WHERE not(person=friend)"
            + " AND not((friend)-[:" + Rels.IS_LOCATED_IN + "]->()-[:" + Rels.IS_LOCATED_IN + "]->(countryX))"
            + " AND countryX." + Place.NAME + "={" + COUNTRY_X + "}"
//            + " AND (messageX:" + Nodes.Comment + " OR messageX:" + Nodes.Post + ")"
            + " AND messageX." + Message.CREATION_DATE + ">={" + MIN_DATE + "} AND messageX." + Message.CREATION_DATE + "<{" + MAX_DATE + "}\n"
            + "WITH friend, count(DISTINCT messageX) AS xCount\n"
            + "MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(messageY)-[:" + Rels.IS_LOCATED_IN + "]->(countryY:" + Place.Type.Country + ")\n"
            + "WHERE countryY." + Place.NAME + "={" + COUNTRY_Y + "}"
            + " AND not((friend)-[:" + Rels.IS_LOCATED_IN + "]->()-[:" + Rels.IS_LOCATED_IN + "]->(countryY))"
//            + " AND (messageY:" + Nodes.Comment + " OR messageY:" + Nodes.Post + ")"
            + " AND messageY." + Message.CREATION_DATE + ">={" + MIN_DATE + "} AND messageY." + Message.CREATION_DATE + "<{" + MAX_DATE + "}\n"
            + "WITH friend." + Person.ID + " AS friendId, friend." + Person.FIRST_NAME + " AS friendFirstName, friend." + Person.LAST_NAME + " AS friendLastName , xCount, count(DISTINCT messageY) AS yCount\n"
            + "RETURN friendId, friendFirstName, friendLastName, xCount, yCount, xCount + yCount AS xyCount\n"
            + "ORDER BY xyCount DESC, friendId ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
