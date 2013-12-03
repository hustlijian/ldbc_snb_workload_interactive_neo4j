package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
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
        queryParams.put( "min_date", params.minDateAsMilli() );
        queryParams.put( "max_date", params.maxDateAsMilli() );
        return queryParams;
    }

    private String query()
    {
        return String.format(

        "MATCH (person:" + Domain.Nodes.Person + ")-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")\n"

        + "WHERE person." + Domain.Person.ID + "={person_id}\n"

        + "MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")\n"

        + "WHERE post." + Domain.Post.CREATION_DATE + ">={min_date} AND post." + Domain.Post.CREATION_DATE
                + "<={max_date}\n"

                + "MATCH (post)-[" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")\n"

                + "WITH DISTINCT tag, collect(tag) AS tags\n"

                + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(tags) AS tagCount\n"

                + "ORDER BY tagCount DESC\n"

                + "LIMIT 10" );
    }
}
