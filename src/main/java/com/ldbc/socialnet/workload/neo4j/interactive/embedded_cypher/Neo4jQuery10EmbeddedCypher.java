package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10Result;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery10;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery10EmbeddedCypher implements Neo4jQuery10 {
    /*
    Q10 - Who to connect with?
        Description
            Users that like me who are not my friends by are my friend's friends.
            Find friends of a friend (excluding me) who post a lot about the interests of the user and little about topics that are not in the interests of the user.
            The search is restricted by the candidate’s horoscope sign.
            The result should contain 10 FOFs, where the difference between the total number of their posts about the interests of the specified user and the total number of their posts
            that are not in the interests of the user, is as large as possible.
            Sort the result descending by the difference mentioned in the previous item, and then ascending by FOF URI.

            Friends of Friends of "person"
            Who have a certain horoscope sign.
            Who post a lot about the interests of "person" and little about topics that are not the interests of "person".
                // "common interests" = number of friend's posts about the interests of "person"
                // "uncommon interests" = number of friend's posts not about the interests of "person"
                // "common interest score" = "common interest" - "uncommon interest"
            Return 10 FOFs,
            Order results descending by "common interest score", then ascending by "friend" ID
        Parameter
            person
            horoscopeSign (a number between 1 and 12)
            horoscopeSign + 1
        Result (for each result return)
            Person.id
            Person.firstName
            Person.lastName
            Person.gender
            Person-isLocatedIn->Location.name (City)
            "common interest score"
     */

    private static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{person_id}})\n"
            + "MATCH (person)-[:" + Rels.KNOWS + "*2..2]-(friend:" + Nodes.Person + ")-[:" + Rels.IS_LOCATED_IN + "]->(city:" + Place.Type.City + ")\n"
            + "WHERE friend." + Person.BIRTHDAY_MONTH + " >= {horoscope_month_min} AND friend." + Person.BIRTHDAY_MONTH + " < {horoscope_month_max}\n"
            + "OPTIONAL MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")\n"
            + "WITH friend, city." + Place.NAME + " AS personCityName, count(post) AS allPostCount, person\n"
            + "OPTIONAL MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")\n"
            + "WHERE (post)-[:" + Rels.HAS_TAG + "]->(:" + Nodes.Tag + ")<-[:" + Rels.HAS_INTEREST + "]-(person)\n"
            + "WITH friend, personCityName, allPostCount, count(post) AS commonPostCount\n"
            + "RETURN"
            + " friend." + Person.ID + " AS personId,"
            + " friend." + Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Person.LAST_NAME + " AS personLastName,"
            + " friend." + Person.GENDER + " AS personGender,"
            + " personCityName,\n"
            + "  CASE allPostCount\n"
            + "   WHEN 0 THEN 0.0\n"
            + "   ELSE commonPostCount / (allPostCount + 0.0)\n"
            + "  END"
            + " AS commonInterestScore\n"
            + "ORDER BY commonInterestScore DESC, personId ASC";

    private static final String QUERY_STRING_ALSO_WORKS = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{person_id}})-[:" + Rels.HAS_INTEREST + "]->(interest:" + Nodes.Tag + ")\n"
            + "WITH person, collect(interest) AS interests\n"
            + "MATCH (person)-[:" + Rels.KNOWS + "*2..2]-(friend:" + Nodes.Person + ")-[:" + Rels.IS_LOCATED_IN + "]->(city:" + Place.Type.City + ")\n"
            + "WHERE friend." + Person.BIRTHDAY_MONTH + " >= {horoscope_month_min} AND friend." + Person.BIRTHDAY_MONTH + " < {horoscope_month_max}\n"
            + "OPTIONAL MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")\n"
            + "WITH"
            + " friend,"
            + " interests,"
            + " city." + Place.NAME + " AS personCityName,"
            + " collect(post) AS posts\n"
            + "WITH"
            + " friend,"
            + " interests,"
            + " personCityName,"
            + " length([post IN posts WHERE any(interest IN interests WHERE (post)-[:" + Rels.HAS_TAG + "]->(interest))]) AS commonPostCount,"
            + " length([post IN posts WHERE none(interest IN interests WHERE (post)-[:" + Rels.HAS_TAG + "]->(interest))]) AS uncommonPostCount\n"
            + "RETURN"
            + " friend." + Person.ID + " AS personId,"
            + " friend." + Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Person.LAST_NAME + " AS personLastName,"
            + " friend." + Person.GENDER + " AS personGender,"
            + " personCityName,\n"
            + "  CASE (commonPostCount + uncommonPostCount)\n"
            + "   WHEN 0 THEN 0.0\n"
            + "   ELSE commonPostCount / (commonPostCount + uncommonPostCount + 0.0)\n"
            + "  END"
            + " AS commonInterestScore\n"
            + "ORDER BY commonInterestScore DESC, personId ASC";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery10Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery10 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery10Result>() {
                    @Override
                    public LdbcQuery10Result apply(Map<String, Object> row) {
                        return new LdbcQuery10Result(
                                (String) row.get("personFirstName"),
                                (String) row.get("personLastName"),
                                (long) row.get("personId"),
                                (double) row.get("commonInterestScore"),
                                (String) row.get("personGender"),
                                (String) row.get("personCityName"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery10 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id", operation.personId());
        queryParams.put("horoscope_month_min", operation.horoscopeMonth1());
        queryParams.put("horoscope_month_max", operation.horoscopeMonth2());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
