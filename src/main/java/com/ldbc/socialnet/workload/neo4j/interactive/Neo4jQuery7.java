package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public abstract class Neo4jQuery7<CONNECTION> implements Neo4jQuery<LdbcQuery7, LdbcQuery7Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer LIMIT = 2;
    /*
    Given a start Person, find (most recent) Likes on any of start Person's Posts/Comments.
    Return top 20 Persons that Liked any of start Person's Posts/Comments, the Post/Comment they liked most recently, creation date of that Like,
    and the latency (in minutes) between creation of Post/Comment and Like.
    Additionally, return a flag indicating whether the liker is a friend of start Person.
    Sort results descending by creation time of Like, then ascending by Person identifier of liker, and finally ascending by Post identifier
    */
    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID + "}})<-[:" + Rels.HAS_CREATOR + "]-(message)<-[like:" + Rels.LIKES + "]-(liker:" + Nodes.Person + ")\n"
            + "WITH liker, message, like." + Likes.CREATION_DATE + " AS likeTime, person\n"
            + "ORDER BY likeTime DESC, message." + Message.ID + " ASC\n"
            + "WITH liker, head(collect({message: message, likeTime: likeTime})) AS latestLike, person\n"
            + "RETURN"
            + " liker." + Person.ID + " AS personId,"
            + " liker." + Person.FIRST_NAME + " AS personFirstName,"
            + " liker." + Person.LAST_NAME + " AS personLastName,"
            + " latestLike.likeTime AS likeTime,"
            + " not((liker)-[:" + Rels.KNOWS + "]-(person)) AS isNew,"
            + " latestLike.message." + Message.ID + " AS messageId,"
            + " latestLike.message." + Message.CONTENT + " AS messageContent,"
            + " latestLike.likeTime - latestLike.message." + Message.CREATION_DATE + " AS latencyAsMilli\n"
            + "ORDER BY likeTime DESC, personId ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
