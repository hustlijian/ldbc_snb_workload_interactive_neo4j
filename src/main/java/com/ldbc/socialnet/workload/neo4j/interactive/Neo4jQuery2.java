package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery2<CONNECTION> implements Neo4jQuery<LdbcQuery2, LdbcQuery2Result, CONNECTION> {
    /*
    QUERY2:
        Find the newest 20 posts from your friends
        
        Find the newest 20 posts from all of your friends, but created before a certain date (including that date). 
        ID of a friend, ID of a post, and creationDate are returned, sorted descending by creationDate, and then ascending by post URI.
        
    FORMALLY:
        Get friends
        Get posts by friends that were made before a certain date (including that date)
        Order by post creation date
        Limit to top 20

    PARAMETERS:
        Person
        Date

    RETURN:    
        Person.Id
        Person.firstName
        Person.lastName
        Post.Id
        Post.content or Post.imageFile
        Post.Date
     */

    protected static final Integer PERSON_ID = 1;
    protected static final Integer MAX_DATE = 2;
    protected static final Integer LIMIT = 3;

    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"
            + "WHERE post." + Domain.Post.CREATION_DATE + " <= {" + MAX_DATE + "}\n"
            + "RETURN"
            + " friend." + Domain.Person.ID + " AS personId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS personLastName,"
            + " post." + Domain.Post.ID + " AS postId,"
            + " post." + Domain.Post.CONTENT + " AS postContent,"
            + " post." + Domain.Post.CREATION_DATE + " AS postDate\n"
            + "ORDER BY postDate DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
