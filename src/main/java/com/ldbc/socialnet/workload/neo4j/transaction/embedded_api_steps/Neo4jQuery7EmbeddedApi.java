package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.traversal.steps.execution.StepsUtils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.socialnet.workload.LdbcQuery7;
import com.ldbc.socialnet.workload.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery7;

import static com.ldbc.socialnet.workload.Domain.*;

public class Neo4jQuery7EmbeddedApi implements Neo4jQuery7
{
    private final LdbcTraversers traversers;

    public Neo4jQuery7EmbeddedApi( LdbcTraversers traversers )
    {
        this.traversers = traversers;
    }

    @Override
    public String description()
    {
        return "LDBC Query7 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery7Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery7 operation )
    {
        /*
        QUERY 7
        
        DESCRIPTION
            Find the 10 most popular tags that occurred in your country during the last X hours, 
            and that none of your friends has discussed.

        FORMALLY
            Find posts in person's country
            Made in last X hours
            That person's friends have not commented on
            Get tags on those posts
            Order tags by "popularity" (count)
            Limit to 10 most popular

        PARAMETERS
            Person
            startDateTime
            duration (hours)
                        
        RETURN
            Tag.name
            count        
         */

        /*
        MATCH (person:Person {id:{person_id}})-[:IS_LOCATED_IN]->(:City)-[:IS_LOCATED_IN]->(country:Country),
        (country)<-[:IS_LOCATED_IN]-(post:Post)-[:HAS_CREATOR]->(creator:Person),
        (post)-[:HAS_TAG]->(tag:Tag)
        WHERE NOT((person)-[:KNOWS]-(creator)) AND post.creationDate>{min_date} AND post.creationDate<{max_date}
        RETURN DISTINCT tag.name AS tagName, count(tag) AS tagCount
        ORDER BY tagCount DESC
        LIMIT 10
         */

        /*
        MATCH (person:Person {id:{person_id}})
         */
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty( Nodes.Person, Person.ID, operation.personId() ).iterator();
        if ( false == personIterator.hasNext() ) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        /*
        MATCH (person)-[:IS_LOCATED_IN]->(:City)-[:IS_LOCATED_IN]->(:Country)<-[:IS_LOCATED_IN]-(post:Post)-[:HAS_CREATOR]->(creator:Person),
        WHERE NOT((person)-[:KNOWS]-(creator)) AND post.creationDate>{min_date} AND post.creationDate<{max_date}
         */
        List<Node> posts = ImmutableList.copyOf( StepsUtils.projectNodesFromPath(
                Iterables.filter(
                        traversers.postsInPersonsCountryInDateRangeNotCreatedByOtherPerson(
                                operation.startDateTimeAsMilli(), operation.endDateTimeAsMilli(), person ).traverse(
                                person ), new Predicate<Path>()
                        {
                            @Override
                            public boolean apply( Path path )
                            {
                                Node creator = path.endNode();
                                for ( Relationship knows : creator.getRelationships( Rels.KNOWS, Direction.BOTH ) )
                                {
                                    if ( knows.getOtherNode( creator ).equals( person ) ) return false;
                                }
                                return true;
                            }
                        } ), 3 ) );

        /*
        MATCH (post)-[:HAS_TAG]->(tag:Tag)
         */
        Iterator<Node> tags = traversers.postsTags().traverse( posts.toArray( new Node[posts.size()] ) ).nodes().iterator();

        /*
        RETURN DISTINCT tag.name AS tagName, count(tag) AS tagCount
         */
        Map<String, Integer> tagCounts = StepsUtils.count( Iterators.transform( tags, new Function<Node, String>()
        {
            @Override
            public String apply( Node tag )
            {
                return (String) tag.getProperty( Tag.NAME );
            }
        } ) );
        List<LdbcQuery7Result> result = Lists.newArrayList( Iterables.transform( tagCounts.entrySet(),
                new Function<Entry<String, Integer>, LdbcQuery7Result>()
                {
                    @Override
                    public LdbcQuery7Result apply( Entry<String, Integer> tagCount )
                    {
                        return new LdbcQuery7Result( tagCount.getKey(), tagCount.getValue() );
                    }
                } ) );

        /*
        ORDER BY tagCount DESC
        LIMIT 10
         */
        Collections.sort( result, new TagCountComparator() );

        return Iterators.limit( result.iterator(), operation.limit() );
    }

    public static class TagCountComparator implements Comparator<LdbcQuery7Result>
    {
        @Override
        public int compare( LdbcQuery7Result result1, LdbcQuery7Result result2 )
        {
            if ( result1.tagCount() == result2.tagCount() ) return 0;
            if ( result1.tagCount() > result2.tagCount() ) return -1;
            return 1;
        }
    }
}
