package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

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
    public Iterator<LdbcQuery3Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery3 params )
    {
        return new ResultIterator( engine.execute(
                query(),
                buildParams( params.personId(), params.countryX(), params.countryY(), params.endDate(),
                        params.durationDays() ) ).iterator() );
    }

    private String query()
    {
        return String.format(

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(f:" + Domain.Node.PERSON + ")\n"

        + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

        + "WHERE person." + Domain.Person.ID + "={person_id}\n"

        + "WITH DISTINCT f AS friend\n"

        + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postX:" + Domain.Node.POST + ")-[:"
                + Domain.Rel.IS_LOCATED_IN + "]->(countryX:" + Domain.Place.Type.COUNTRY + ")\n"

                + "USING INDEX countryX:" + Domain.Place.Type.COUNTRY + "(" + Domain.Place.NAME + ")\n"

                + "WHERE countryX." + Domain.Place.NAME + "={country_x} AND postX." + Domain.Post.CREATION_DATE
                + ">={min_date} AND postX." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                + "WITH friend, count(DISTINCT postX) AS xCount\n"

                + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postY:" + Domain.Node.POST + ")-[:"
                + Domain.Rel.IS_LOCATED_IN + "]->(countryY:" + Domain.Place.Type.COUNTRY + ")\n"

                + "USING INDEX countryY:" + Domain.Place.Type.COUNTRY + "(" + Domain.Place.NAME + ")\n"

                + "WHERE countryY." + Domain.Place.NAME + "={country_y} AND postY." + Domain.Post.CREATION_DATE
                + ">={min_date} AND postY." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                + "WITH friend." + Domain.Person.FIRST_NAME + " + ' ' + friend." + Domain.Person.LAST_NAME
                + "  AS friendName , xCount, count(DISTINCT postY) AS yCount\n"

                + "RETURN friendName, xCount, yCount, xCount + yCount AS xyCount\n"

                + "ORDER BY xyCount DESC" );
    }

    private Map<String, Object> buildParams( long personId, String countryX, String countryY, Date endDate,
            int durationDays )
    {
        long maxDateInMilli = endDate.getTime();
        Calendar c = Calendar.getInstance();
        c.setTime( endDate );
        c.add( Calendar.DATE, -durationDays );
        long minDateInMilli = c.getTimeInMillis();
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", personId );
        queryParams.put( "country_x", countryX );
        queryParams.put( "country_y", countryY );
        queryParams.put( "min_date", minDateInMilli );
        queryParams.put( "max_date", maxDateInMilli );
        return queryParams;
    }

    public static class ResultIterator implements Iterator<LdbcQuery3Result>
    {
        private final Iterator<Map<String, Object>> inner;

        public ResultIterator( Iterator<Map<String, Object>> inner )
        {
            this.inner = inner;
        }

        @Override
        public boolean hasNext()
        {
            return inner.hasNext();
        }

        @Override
        public LdbcQuery3Result next()
        {
            Map<String, Object> next = inner.next();
            return new LdbcQuery3Result( (String) next.get( "friendName" ), (long) next.get( "xCount" ),
                    (long) next.get( "yCount" ), (long) next.get( "xyCount" ) );
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

    }
}
