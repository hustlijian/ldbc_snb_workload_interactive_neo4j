package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery7;
import com.ldbc.socialnet.workload.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery7;

public class Neo4jQuery7EmbeddedCypher implements Neo4jQuery7
{
    @Override
    public String description()
    {
        return query();
    }

    @Override
    public Iterator<LdbcQuery7Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery7 operation )
    {
        return Iterators.transform( engine.execute( query(), buildParams( operation ) ).iterator(),
                new Function<Map<String, Object>, LdbcQuery7Result>()
                {
                    @Override
                    public LdbcQuery7Result apply( Map<String, Object> next )
                    {
                        return new LdbcQuery7Result( (String) next.get( "tagName" ), (long) next.get( "tagCount" ) );
                    }
                } );
    }

    private Map<String, Object> buildParams( LdbcQuery7 operation )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", operation.personId() );
        queryParams.put( "min_date", operation.startDateTimeAsMilli() );
        queryParams.put( "max_date", operation.endDateTimeAsMilli() );
        return queryParams;
    }

    // TODO complete implementation
    private String query()
    {
        return "MATCH (person:" + Domain.Node.Person + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(:"
               + Domain.Place.Type.City + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(country:"
               + Domain.Place.Type.Country + ")\n"

               + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.ID + ")\n"

               + "WHERE person." + Domain.Person.ID + "={person_id}\n"

               + "WITH person, country\n"

               + "MATCH (person)-[:" + Domain.Rel.KNOWS + "]->(friend:" + Domain.Node.Person + ")\n"

               + "WITH friend, country\n"

               + "MATCH (country)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(post:" + Domain.Node.Post + ")-[:"
               + Domain.Rel.HAS_TAG + "]->(tag:" + Domain.Node.Tag + ")\n"

               // + "WHERE NOT((tag)<-[:" + Domain.Rel.HAS_TAG + "]-(:" +
               // Domain.Node.POST + ")-[:"
               // + Domain.Rel.HAS_CREATOR + "]->(friend))\n"

               + "RETURN tag.name AS tag, post.content AS post, country.name AS country\n"

        // + "RETURN DISTINCT tag.name AS tag, count(tag) AS count\n"
        //
        // + "ORDER BY count DESC\n"
        //
        // + "LIMIT 10"
        ;

    }

}
