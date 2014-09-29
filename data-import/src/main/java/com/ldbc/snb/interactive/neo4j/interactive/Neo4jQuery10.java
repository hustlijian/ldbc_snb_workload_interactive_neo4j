package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;

import static com.ldbc.snb.interactive.neo4j.Domain.*;

public abstract class Neo4jQuery10<CONNECTION> implements Neo4jQuery<LdbcQuery10, LdbcQuery10Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer MONTH = 2;
    protected static final Integer LIMIT = 4;

    /*
    Given a start Person, find that Person's friends of friends (excluding start Person, and immediate friends),
    who were born on or after the 21st of a given month (in any year) and before the 22nd of the following month.
    Calculate the similarity between each of these Persons and start Person, where similarity for any Person is defined as follows:
      - common = number of Posts created by that Person, such that the Post has a Tag that start Person is Interested in
      - uncommon = number of Posts created by that Person, such that the Post has no Tag that start Person is Interested in
      - similarity = common - uncommon
    Return top 10 Persons, their Location, and their similarity score.
    Sort results descending by similarity score, and then ascending by Person identifier
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID + "}})-[:" + Rels.KNOWS + "*2..2]-(friend:" + Nodes.Person + ")-[:" + Rels.IS_LOCATED_IN + "]->(city:" + Place.Type.City + ")\n"
            + "WHERE"
            + " ((friend." + Person.BIRTHDAY_MONTH + " = {" + MONTH + "} AND friend." + Person.BIRTHDAY_DAY_OF_MONTH + " >= 21) OR (friend." + Person.BIRTHDAY_MONTH + " = ({" + MONTH + "}+1)%12 AND friend." + Person.BIRTHDAY_DAY_OF_MONTH + " < 22))"
            + " AND not(friend=person) AND not((friend)-[:" + Rels.KNOWS + "]-(person))\n"
            + "WITH DISTINCT friend, city, person\n"
            + "OPTIONAL MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")\n"
            + "WITH friend, city, collect(post) AS posts, person\n"
            + "WITH friend, city, length(posts) AS postCount, length([p IN posts WHERE (p)-[:" + Rels.HAS_TAG + "]->(:" + Nodes.Tag + ")<-[:" + Rels.HAS_INTEREST + "]-(person)]) AS commonPostCount\n"
            + "RETURN"
            + " friend." + Person.ID + " AS personId,"
            + " friend." + Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Person.LAST_NAME + " AS personLastName,"
            + " friend." + Person.GENDER + " AS personGender,"
            + " city." + Place.NAME + " AS personCityName,"
            + " commonPostCount - (postCount - commonPostCount) AS commonInterestScore\n"
            + "ORDER BY commonInterestScore DESC, personId ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
