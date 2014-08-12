package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery10<CONNECTION> implements Neo4jQuery<LdbcQuery10, LdbcQuery10Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer HOROSCOPE_MONTH_MIN = 2;
    protected static final Integer HOROSCOPE_MONTH_MAX = 3;
    protected static final Integer LIMIT = 4;

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})\n"
            + "MATCH (person)-[:" + Domain.Rels.KNOWS + "*2..2]-(friend:" + Domain.Nodes.Person + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(city:" + Domain.Place.Type.City + ")\n"
            + "WHERE friend." + Domain.Person.BIRTHDAY_MONTH + " >= {" + HOROSCOPE_MONTH_MIN + "} AND friend." + Domain.Person.BIRTHDAY_MONTH + " < {" + HOROSCOPE_MONTH_MAX + "}\n"
            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"
            + "WITH friend, city." + Domain.Place.NAME + " AS personCityName, count(post) AS allPostCount, person\n"
            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"
            + "WHERE (post)-[:" + Domain.Rels.HAS_TAG + "]->(:" + Domain.Nodes.Tag + ")<-[:" + Domain.Rels.HAS_INTEREST + "]-(person)\n"
            + "WITH friend, personCityName, allPostCount, count(post) AS commonPostCount\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS personId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS personLastName,"
            + " friend." + Domain.Person.GENDER + " AS personGender,"
            + " personCityName,\n"
            + "  CASE allPostCount\n"
            + "   WHEN 0 THEN 0.0\n"
            + "   ELSE commonPostCount / (allPostCount + 0.0)\n"
            + "  END"
            + " AS commonInterestScore\n"
            + "ORDER BY commonInterestScore DESC, personId ASC\n"
            + "LIMIT {" + LIMIT + "}";

//    protected static final String QUERY_STRING_ALSO_WORKS = ""
//            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id}})-[:" + Domain.Rels.HAS_INTEREST + "]->(interest:" + Domain.Nodes.Tag + ")\n"
//            + "WITH person, collect(interest) AS interests\n"
//            + "MATCH (person)-[:" + Domain.Rels.KNOWS + "*2..2]-(friend:" + Domain.Nodes.Person + ")-[:" + Domain.Rels.IS_LOCATED_IN + "]->(city:" + Domain.Place.Type.City + ")\n"
//            + "WHERE friend." + Domain.Person.BIRTHDAY_MONTH + " >= {horoscope_month_min} AND friend." + Domain.Person.BIRTHDAY_MONTH + " < {horoscope_month_max}\n"
//            + "OPTIONAL MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"
//            + "WITH"
//            + " friend,"
//            + " interests,"
//            + " city." + Domain.Place.NAME + " AS personCityName,"
//            + " collect(post) AS posts\n"
//            + "WITH"
//            + " friend,"
//            + " interests,"
//            + " personCityName,"
//            + " length([post IN posts WHERE any(interest IN interests WHERE (post)-[:" + Domain.Rels.HAS_TAG + "]->(interest))]) AS commonPostCount,"
//            + " length([post IN posts WHERE none(interest IN interests WHERE (post)-[:" + Domain.Rels.HAS_TAG + "]->(interest))]) AS uncommonPostCount\n"
//            + "RETURN"
//            + " friend." + Domain.Person.ID + " AS personId,"
//            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
//            + " friend." + Domain.Person.LAST_NAME + " AS personLastName,"
//            + " friend." + Domain.Person.GENDER + " AS personGender,"
//            + " personCityName,\n"
//            + "  CASE (commonPostCount + uncommonPostCount)\n"
//            + "   WHEN 0 THEN 0.0\n"
//            + "   ELSE commonPostCount / (commonPostCount + uncommonPostCount + 0.0)\n"
//            + "  END"
//            + " AS commonInterestScore\n"
//            + "ORDER BY commonInterestScore DESC, personId ASC";
}
