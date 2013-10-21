package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery4;
import com.ldbc.socialnet.workload.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery4;
import com.ldbc.socialnet.workload.neo4j.traversal.TraversalUtils;

public class Neo4jQuery4EmbeddedApi_OLD implements Neo4jQuery4
{
    @Override
    public String description()
    {
        return "LDBC Query4 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery4Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery4 params )
    {
        /*
        Find the top 10 most popular topics/tags (by the number of comments and posts) that your friends have 
        been talking about in the last x hours.
        
        MATCH (person:PERSON)-[:KNOWS]-(friend:PERSON)
        USING INDEX person:PERSON(id)
        WHERE person.id={person_id}
        WITH friend
        MATCH (friend)<-[:HAS_CREATOR]-(post:POST)
        WHERE post.creationDate>={min_date} AND post.creationDate<={max_date}
        WITH post
        MATCH (post)-[HAS_TAG]->(tag:TAG)
        WITH DISTINCT tag, collect(tag) AS tags
        RETURN tag.name AS tagName, length(tags) AS tagCount
        ORDER BY tagCount DESC
        LIMIT 10         
        */
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty( Domain.Node.Person, Domain.Person.ID,
                params.personId() ).iterator();
        if ( false == personIterator.hasNext() ) return Iterators.emptyIterator();
        Node person = personIterator.next();
        Iterator<String> tagNames = Iterators.transform(
                LdbcTraversers_OLD.friendPostTags( params.startDateAsMilli(), params.endDateAsMilli() ).traverse( person ).nodes().iterator(),
                new Function<Node, String>()
                {
                    @Override
                    public String apply( Node endNode )
                    {
                        return (String) endNode.getProperty( Domain.Tag.NAME );
                    }
                } );
        Map<String, Integer> tagNamesCountMap = TraversalUtils.count( tagNames );
        List<LdbcQuery4Result> tagCounts = Lists.newArrayList( Iterables.transform( tagNamesCountMap.entrySet(),
                new Function<Entry<String, Integer>, LdbcQuery4Result>()
                {
                    @Override
                    public LdbcQuery4Result apply( Entry<String, Integer> input )
                    {
                        return new LdbcQuery4Result( input.getKey(), input.getValue() );
                    }
                } ) );
        Collections.sort( tagCounts, new TagCountComparator() );
        return Iterators.limit( tagCounts.iterator(), 10 );
    }

    public static class TagCountComparator implements Comparator<LdbcQuery4Result>
    {
        @Override
        public int compare( LdbcQuery4Result result1, LdbcQuery4Result result2 )
        {
            if ( result1.tagCount() == result2.tagCount() ) return 0;
            if ( result1.tagCount() > result2.tagCount() ) return -1;
            return 1;
        }
    }
}
