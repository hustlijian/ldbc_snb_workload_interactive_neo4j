package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

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
    public Iterator<LdbcQuery7Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery7 params )
    {
        return new ResultIterator( engine.execute( query(),
                buildParams( params.personId(), params.endDateTime(), params.durationHours() ) ).iterator() );
    }

    private Map<String, Object> buildParams( long personId, Date endDateTime, int durationHours )
    {
        long maxDateInMilli = endDateTime.getTime();
        Calendar c = Calendar.getInstance();
        c.setTime( endDateTime );
        c.add( Calendar.HOUR, -durationHours );
        long minDateInMilli = c.getTimeInMillis();

        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", personId );
        queryParams.put( "min_date", minDateInMilli );
        queryParams.put( "max_date", maxDateInMilli );
        return queryParams;
    }

    // TODO complete implementation
    private String query()
    {
        return "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(:"
               + Domain.Place.Type.CITY + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(country:"
               + Domain.Place.Type.COUNTRY + ")\n"

               + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

               + "WHERE person." + Domain.Person.ID + "={person_id}\n"

               + "WITH person, country\n"

               + "MATCH (person)-[:" + Domain.Rel.KNOWS + "]->(friend:" + Domain.Node.PERSON + ")\n"

               + "WITH friend, country\n"

               + "MATCH (country)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(post:" + Domain.Node.POST + ")-[:"
               + Domain.Rel.HAS_TAG + "]->(tag:" + Domain.Node.TAG + ")\n"

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

    public static class ResultIterator implements Iterator<LdbcQuery7Result>
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
        public LdbcQuery7Result next()
        {
            Map<String, Object> next = inner.next();
            return new LdbcQuery7Result( (String) next.get( "tagName" ), (long) next.get( "tagCount" ) );
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

    }
}
