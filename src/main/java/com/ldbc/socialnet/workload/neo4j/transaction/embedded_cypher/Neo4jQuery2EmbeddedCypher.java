package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.LdbcQuery2;
import com.ldbc.socialnet.workload.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery2;

public class Neo4jQuery2EmbeddedCypher implements Neo4jQuery2
{
    @Override
    public String description()
    {
        return query();
    }

    @Override
    public Iterator<LdbcQuery2Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery2 operation )
    {
        return Iterators.transform( engine.execute( query(), buildParams( operation ) ).iterator(),
                new Function<Map<String, Object>, LdbcQuery2Result>()
                {
                    @Override
                    public LdbcQuery2Result apply( Map<String, Object> input )
                    {
                        return new LdbcQuery2Result( (long) input.get( "personId" ),
                                (String) input.get( "personFirstName" ), (String) input.get( "personLastName" ),
                                (long) input.get( "postId" ), (String) input.get( "postContent" ),
                                (long) input.get( "postDate" ) );
                    }
                } );
    }

    private Map<String, Object> buildParams( LdbcQuery2 operation )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", operation.personId() );
        queryParams.put( "max_date", operation.maxDateAsMilli() );
        queryParams.put( "limit", operation.limit() );
        return queryParams;
    }

    private String query()
    {
        return String.format(

        "MATCH (:Person {id:{person_id}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)\n"

        + "WHERE post.creationDate<={max_date}\n"

        + "RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName,\n"

        + "  post.id AS postId, post.content AS postContent, post.creationDate AS postDate\n"

        + "ORDER BY postDate DESC\n"

        + "LIMIT {limit}\n" );
    }
}
