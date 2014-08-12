package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery3<CONNECTION> implements Neo4jQuery<LdbcQuery3, LdbcQuery3Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer COUNTRY_X = 2;
    protected static final Integer COUNTRY_Y = 3;
    protected static final Integer MIN_DATE = 4;
    protected static final Integer MAX_DATE = 5;
    protected static final Integer LIMIT = 6;

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "*1..2]-(friend:" + Domain.Nodes.Person + ")"
            + "<-[:" + Domain.Rels.HAS_CREATOR + "]-(postX:" + Domain.Nodes.Post + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(countryX:" + Domain.Place.Type.Country + ")\n"
            + "WHERE countryX." + Domain.Place.NAME + "={" + COUNTRY_X + "} AND postX." + Domain.Post.CREATION_DATE + ">={" + MIN_DATE + "} AND postX." + Domain.Post.CREATION_DATE + "<={" + MAX_DATE + "}\n"
            + "WITH friend, count(DISTINCT postX) AS xCount\n"
            + "MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(postY:" + Domain.Nodes.Post + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(countryY:" + Domain.Place.Type.Country + " {" + Domain.Place.NAME + ":{" + COUNTRY_Y + "}})\n"
            + "WHERE postY." + Domain.Post.CREATION_DATE + ">={" + MIN_DATE + "} AND postY." + Domain.Post.CREATION_DATE + "<={" + MAX_DATE + "}\n"
            + "WITH friend." + Domain.Person.FIRST_NAME + " + ' ' + friend." + Domain.Person.LAST_NAME + " AS friendName , xCount, count(DISTINCT postY) AS yCount\n"
            + "RETURN friendName, xCount, yCount, xCount + yCount AS xyCount\n"
            + "ORDER BY xyCount DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
