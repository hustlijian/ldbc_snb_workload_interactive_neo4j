package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;

import static com.ldbc.snb.interactive.neo4j.Domain.*;

public abstract class Neo4jQuery5<CONNECTION> implements Neo4jQuery<LdbcQuery5, LdbcQuery5Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer JOIN_DATE = 2;
    protected static final Integer LIMIT = 3;

    /*
    Given a start Person, find the Forums which that Person's friends and friends of friends (excluding start Person) became Members of after a given date.
    Return top 20 Forums, and the number of Posts in each Forum that was Created by any of these Persons - for each Forum consider only those Persons which joined that particular Forum after the given date.
    Sort results descending by the count of Posts, and then ascending by Forum name
     */
    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID + "}})-[:" + Rels.KNOWS + "*1..2]-(friend:" + Nodes.Person + ")<-[membership:" + Rels.HAS_MEMBER + "]-(forum:" + Nodes.Forum + ")\n"
            + "WHERE membership." + HasMember.JOIN_DATE + ">{" + JOIN_DATE + "} AND not(person=friend)\n"
            + "WITH DISTINCT friend, forum\n"
            + "OPTIONAL MATCH (friend)<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + ")<-[:" + Rels.CONTAINER_OF + "]-(forum)\n"
            + "WITH forum, count(post) AS postCount\n"
            + "RETURN forum." + Forum.TITLE + " AS forumName, postCount\n"
            + "ORDER BY postCount DESC, forum." + Forum.ID + " ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
