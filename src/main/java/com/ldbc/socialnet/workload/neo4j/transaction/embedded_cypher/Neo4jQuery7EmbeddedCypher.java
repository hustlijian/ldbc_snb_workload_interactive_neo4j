package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.LdbcQuery7;
import com.ldbc.socialnet.workload.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery7;
import static com.ldbc.socialnet.workload.Domain.*;

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

    private String query()
    {
        return "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{person_id}})-[:" + Rels.IS_LOCATED_IN + "]->(:"
               + Place.Type.City + ")-[:" + Rels.IS_LOCATED_IN + "]->(country:" + Place.Type.Country + "),\n"

               + "  (country)<-[:" + Rels.IS_LOCATED_IN + "]-(post:" + Nodes.Post + ")-[:" + Rels.HAS_CREATOR
               + "]->(creator:" + Nodes.Person + "),\n"

               + "  (post)-[:" + Rels.HAS_TAG + "]->(tag:" + Nodes.Tag + ")\n"

               + "WHERE NOT((person)-[:" + Rels.KNOWS + "]-(creator)) AND post." + Post.CREATION_DATE
               + ">{min_date} AND post." + Post.CREATION_DATE + "<{max_date}\n"

               + "RETURN DISTINCT tag.name AS tagName, count(tag) AS tagCount\n"

               + "ORDER BY tagCount DESC\n"

               + "LIMIT 10";
    }
}
