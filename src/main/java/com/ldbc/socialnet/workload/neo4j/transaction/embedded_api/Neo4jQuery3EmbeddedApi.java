package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery3;
import com.ldbc.socialnet.workload.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery3;

public class Neo4jQuery3EmbeddedApi implements Neo4jQuery3
{
    @Override
    public String description()
    {
        return "LDBC Query3 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery3Result> execute( GraphDatabaseService db, ExecutionEngine engine,
            final LdbcQuery3 operation )
    {
        /*
        Find friends and friends-of-friends of some user, such that those friends have been to (have made posts in) 
        the countries countryX and countryY within a specified period.

        MATCH (person:PERSON)-[:KNOWS*1..2]-(f:PERSON)
        USING INDEX person:PERSON(id)
        WHERE person.id={person_id}
        WITH DISTINCT f AS friend
        MATCH (friend)<-[:HAS_CREATOR]-(postX:POST)-[:IS_LOCATED_IN]->(countryX:COUNTRY)
        USING INDEX countryX:COUNTRY(name)
        WHERE countryX.name={country_x} AND postX.creationDate>={min_date} AND postX.creationDate<={max_date}
        WITH friend, count(DISTINCT postX) AS xCount
        MATCH (friend)<-[:HAS_CREATOR]-(postY:POST)-[:IS_LOCATED_IN]->(countryY:COUNTRY)
        USING INDEX countryY:COUNTRY(name)
        WHERE countryY.name={country_y} AND postY.creationDate>={min_date} AND postY.creationDate<={max_date}
        WITH friend.firstName + ' ' + friend.lastName  AS friendName , xCount, count(DISTINCT postY) AS yCount
        RETURN friendName, xCount, yCount, xCount + yCount AS xyCount
        ORDER BY xyCount DESC
         */
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty( Domain.Node.PERSON, Domain.Person.ID,
                operation.personId() ).iterator();
        if ( false == personIterator.hasNext() ) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        Iterator<Node> friendsWithPerson = TraversalUtils.distinct( LdbcTraversers.friendsAndFriendsOfFriends().traverse(
                person ).nodes().iterator() );
        Iterator<Node> friends = Iterators.filter( TraversalUtils.distinct( friendsWithPerson ), new Predicate<Node>()
        {
            @Override
            public boolean apply( Node input )
            {
                return !input.equals( person );
            }
        } );

        Iterator<LdbcQuery3Result> resultWithZeroCounts = Iterators.transform( friends,
                new Function<Node, LdbcQuery3Result>()
                {
                    @Override
                    public LdbcQuery3Result apply( Node friend )
                    {
                        int countryXPostCount = Iterators.size( LdbcTraversers.postsInCountry( operation.countryX(),
                                operation.startDateAsMilli(), operation.endDateAsMilli() ).traverse( friend ).iterator() );
                        int countryYPostCount = Iterators.size( LdbcTraversers.postsInCountry( operation.countryY(),
                                operation.startDateAsMilli(), operation.endDateAsMilli() ).traverse( friend ).iterator() );
                        String friendName = friend.getProperty( Domain.Person.FIRST_NAME ) + " "
                                            + friend.getProperty( Domain.Person.LAST_NAME );
                        return new LdbcQuery3Result( friendName, countryXPostCount, countryYPostCount );
                    }
                } );
        List<LdbcQuery3Result> result = Lists.newArrayList( Iterators.filter( resultWithZeroCounts,
                new Predicate<LdbcQuery3Result>()
                {
                    @Override
                    public boolean apply( LdbcQuery3Result input )
                    {
                        return input.xCount() > 0 && input.yCount() > 0;
                    }
                } ) );
        Collections.sort( result, new CountComparator() );
        return result.iterator();
    }

    public static class CountComparator implements Comparator<LdbcQuery3Result>
    {
        @Override
        public int compare( LdbcQuery3Result result1, LdbcQuery3Result result2 )
        {
            if ( result1.xyCount() == result2.xyCount() ) return 0;
            if ( result1.xyCount() > result2.xyCount() ) return -1;
            return 1;
        }
    }

}