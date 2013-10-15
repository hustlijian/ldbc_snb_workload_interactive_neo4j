package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

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
    public Iterator<LdbcQuery4Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery4 params )
    {
        return new ResultIterator( engine.execute( query(),
                buildParams( params.personId(), params.endDate(), params.durationDays() ) ).iterator() );
    }

    private Map<String, Object> buildParams( long personId, Date endDate, int durationDays )
    {
        Calendar c = Calendar.getInstance();
        c.setTime( endDate );
        c.add( Calendar.DATE, -durationDays );
        long minDateInMilli = c.getTimeInMillis();
        long maxDateInMilli = endDate.getTime();

        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", personId );
        queryParams.put( "min_date", minDateInMilli );
        queryParams.put( "max_date", maxDateInMilli );
        return queryParams;
    }

    private String query()
    {
        return String.format(

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "]-(friend:" + Domain.Node.PERSON + ")\n"

        + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

        + "WHERE person." + Domain.Person.ID + "={person_id}\n"

        + "WITH friend\n"

        + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.POST + ")\n"

        + "WHERE post." + Domain.Post.CREATION_DATE + ">={min_date} AND post." + Domain.Post.CREATION_DATE
                + "<={max_date}\n"

                + "WITH post\n"

                + "MATCH (post)-[" + Domain.Rel.HAS_TAG + "]->(tag:" + Domain.Node.TAG + ")\n"

                + "WITH DISTINCT tag, collect(tag) AS tags\n"

                + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(tags) AS tagCount\n"

                + "ORDER BY tagCount DESC\n"

                + "LIMIT 10" );
    }

    public static class ResultIterator implements Iterator<LdbcQuery4Result>
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
        public LdbcQuery4Result next()
        {
            Map<String, Object> next = inner.next();
            return new LdbcQuery4Result( (String) next.get( "tagName" ), (int) next.get( "tagCount" ) );
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

    }
}
