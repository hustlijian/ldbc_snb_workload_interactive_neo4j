package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery3;
import com.ldbc.socialnet.workload.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery3;

public class Neo4jQuery3EmbeddedCypher implements Neo4jQuery3
{
    @Override
    public String description()
    {
        return query();
    }

    @Override
    public Iterator<LdbcQuery3Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery3 operation )
    {
        return Iterators.transform( engine.execute( query(), buildParams( operation ) ).iterator(),
                new Function<Map<String, Object>, LdbcQuery3Result>()
                {
                    @Override
                    public LdbcQuery3Result apply( Map<String, Object> input )
                    {
                        return new LdbcQuery3Result( (String) input.get( "friendName" ), (long) input.get( "xCount" ),
                                (long) input.get( "yCount" ) );
                    }
                } );
    }

    private String query()
    {
        return String.format(

        "MATCH (person:" + Domain.Node.Person + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(f:" + Domain.Node.Person + ")\n"

        + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.ID + ")\n"

        + "WHERE person." + Domain.Person.ID + "={person_id}\n"

        + "WITH DISTINCT f AS friend\n"

        + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postX:" + Domain.Node.Post + ")-[:"
                + Domain.Rel.IS_LOCATED_IN + "]->(countryX:" + Domain.Place.Type.Country + ")\n"

                + "USING INDEX countryX:" + Domain.Place.Type.Country + "(" + Domain.Place.NAME + ")\n"

                + "WHERE countryX." + Domain.Place.NAME + "={country_x} AND postX." + Domain.Post.CREATION_DATE
                + ">={min_date} AND postX." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                + "WITH friend, count(DISTINCT postX) AS xCount\n"

                + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postY:" + Domain.Node.Post + ")-[:"
                + Domain.Rel.IS_LOCATED_IN + "]->(countryY:" + Domain.Place.Type.Country + ")\n"

                + "USING INDEX countryY:" + Domain.Place.Type.Country + "(" + Domain.Place.NAME + ")\n"

                + "WHERE countryY." + Domain.Place.NAME + "={country_y} AND postY." + Domain.Post.CREATION_DATE
                + ">={min_date} AND postY." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                + "WITH friend." + Domain.Person.FIRST_NAME + " + ' ' + friend." + Domain.Person.LAST_NAME
                + "  AS friendName , xCount, count(DISTINCT postY) AS yCount\n"

                + "RETURN friendName, xCount, yCount, xCount + yCount AS xyCount\n"

                + "ORDER BY xyCount DESC" );
        // return String.format(
        //
        // "MATCH (person:" + Domain.Node.Person + ")-[:" + Domain.Rel.KNOWS +
        // "*1..2]-(f:" + Domain.Node.Person + ")\n"
        //
        // + "WHERE person." + Domain.Person.ID + "={person_id}\n"
        //
        // + "WITH DISTINCT f AS friend\n"
        //
        // + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postX:" +
        // Domain.Node.Post + ")\n"
        //
        // + "WHERE postX." + Domain.Post.CREATION_DATE +
        // ">={min_date} AND postX." + Domain.Post.CREATION_DATE
        // + "<={max_date}\n"
        //
        // + "MATCH (postX)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(countryX:" +
        // Domain.Place.Type.Country + ")\n"
        //
        // + "WHERE countryX." + Domain.Place.NAME + "={country_x}\n"
        //
        // + "WITH friend, count(DISTINCT postX) AS xCount\n"
        //
        // + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postY:" +
        // Domain.Node.Post + ")\n"
        //
        // + "WHERE postY." + Domain.Post.CREATION_DATE +
        // ">={min_date} AND postY." + Domain.Post.CREATION_DATE
        // + "<={max_date}\n"
        //
        // + "MATCH (postY)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(countryY:" +
        // Domain.Place.Type.Country + ")\n"
        //
        // + "WHERE countryY." + Domain.Place.NAME + "={country_y}\n"
        //
        // + "WITH friend." + Domain.Person.FIRST_NAME + " + ' ' + friend." +
        // Domain.Person.LAST_NAME
        // + "  AS friendName , xCount, count(DISTINCT postY) AS yCount\n"
        //
        // + "RETURN friendName, xCount, yCount, xCount + yCount AS xyCount\n"
        //
        // + "ORDER BY xyCount DESC" );
    }

    private Map<String, Object> buildParams( LdbcQuery3 operation )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", operation.personId() );
        queryParams.put( "country_x", operation.countryX() );
        queryParams.put( "country_y", operation.countryY() );
        queryParams.put( "min_date", operation.startDateAsMilli() );
        queryParams.put( "max_date", operation.endDateAsMilli() );
        return queryParams;
    }
}
