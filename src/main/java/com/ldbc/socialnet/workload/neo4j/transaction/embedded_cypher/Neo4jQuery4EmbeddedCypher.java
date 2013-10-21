package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery4;
import com.ldbc.socialnet.workload.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery4;

public class Neo4jQuery4EmbeddedCypher implements Neo4jQuery4
{
    @Override
    public String description()
    {
        return query();
    }

    @Override
    public Iterator<LdbcQuery4Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery4 operation )
    {
        return Iterators.transform( engine.execute( query(), buildParams( operation ) ).iterator(),
                new Function<Map<String, Object>, LdbcQuery4Result>()
                {
                    @Override
                    public LdbcQuery4Result apply( Map<String, Object> input )
                    {
                        return new LdbcQuery4Result( (String) input.get( "tagName" ), (int) input.get( "tagCount" ) );
                    }
                } );
    }

    private Map<String, Object> buildParams( LdbcQuery4 params )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", params.personId() );
        queryParams.put( "min_date", params.startDateAsMilli() );
        queryParams.put( "max_date", params.endDateAsMilli() );
        return queryParams;
    }

    private String query()
    {
        return String.format(

        "MATCH (person:" + Domain.Node.Person + ")-[:" + Domain.Rel.KNOWS + "]-(friend:" + Domain.Node.Person + ")\n"

        + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.ID + ")\n"

        + "WHERE person." + Domain.Person.ID + "={person_id}\n"

        + "WITH friend\n"

        + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.Post + ")\n"

        + "WHERE post." + Domain.Post.CREATION_DATE + ">={min_date} AND post." + Domain.Post.CREATION_DATE
                + "<={max_date}\n"

                + "WITH post\n"

                + "MATCH (post)-[" + Domain.Rel.HAS_TAG + "]->(tag:" + Domain.Node.Tag + ")\n"

                + "WITH DISTINCT tag, collect(tag) AS tags\n"

                + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(tags) AS tagCount\n"

                + "ORDER BY tagCount DESC\n"

                + "LIMIT 10" );
    }
}
