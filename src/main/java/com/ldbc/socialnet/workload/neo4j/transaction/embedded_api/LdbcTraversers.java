package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api;

import java.util.Collections;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.ldbc.driver.util.Function1;
import com.ldbc.socialnet.workload.Domain;

public class LdbcTraversers
{
    // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
    // uni.name, uniCity.name(studyAt.classYear)
    public static TraversalDescription personUniversities()
    {
        Function1<Path, Iterable<Relationship>> step1 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                return from.endNode().getRelationships( Domain.Rel.STUDY_AT, Direction.OUTGOING );
            }
        };
        Function1<Path, Iterable<Relationship>> step2 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                Node endNode = from.endNode();
                if ( false == endNode.hasLabel( Domain.Organisation.Type.UNIVERSITY ) ) return Collections.emptyList();
                return Iterables.filter( endNode.getRelationships( Domain.Rel.IS_LOCATED_IN, Direction.OUTGOING ),
                        new Predicate<Relationship>()
                        {
                            @Override
                            public boolean apply( Relationship input )
                            {
                                return input.getEndNode().hasLabel( Domain.Place.Type.CITY );
                            }
                        } );
            }
        };
        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator( Evaluators.atDepth( 2 ) ).expand(
                new StepsExpander( step1, step2 ) );
    }

    // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
    // company.name, companyCountry.name(worksAt.workFrom)
    public static TraversalDescription personCompanies()
    {
        Function1<Path, Iterable<Relationship>> step1 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                return from.endNode().getRelationships( Domain.Rel.WORKS_AT, Direction.OUTGOING );
            }
        };
        Function1<Path, Iterable<Relationship>> step2 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                Node endNode = from.endNode();
                if ( false == endNode.hasLabel( Domain.Organisation.Type.COMPANY ) ) return Collections.emptyList();
                return Iterables.filter( endNode.getRelationships( Domain.Rel.IS_LOCATED_IN, Direction.OUTGOING ),
                        new Predicate<Relationship>()
                        {
                            @Override
                            public boolean apply( Relationship input )
                            {
                                return input.getEndNode().hasLabel( Domain.Place.Type.COUNTRY );
                            }
                        } );
            }
        };
        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator( Evaluators.atDepth( 2 ) ).expand(
                new StepsExpander( step1, step2 ) );
    }

    /*
        MATCH (person:PERSON)-[:KNOWS]-(friend:PERSON)
        MATCH (friend)<-[:HAS_CREATOR]-(post:POST)
        WHERE post.creationDate>={min_date} AND post.creationDate<={max_date}
        MATCH (post)-[HAS_TAG]->(tag:TAG)
     */
    public static TraversalDescription friendPostTags( final long minDate, final long maxDate )
    {
        Function1<Path, Iterable<Relationship>> step1 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                return from.endNode().getRelationships( Domain.Rel.KNOWS, Direction.BOTH );
            }
        };
        Function1<Path, Iterable<Relationship>> step2 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                Node endNode = from.endNode();
                if ( false == endNode.hasLabel( Domain.Node.PERSON ) ) return Collections.emptyList();
                return endNode.getRelationships( Domain.Rel.HAS_CREATOR, Direction.INCOMING );
            }
        };
        Function1<Path, Iterable<Relationship>> step3 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                Node endNode = from.endNode();
                if ( false == endNode.hasLabel( Domain.Node.POST ) ) return Collections.emptyList();
                long creationDate = (long) endNode.getProperty( Domain.Post.CREATION_DATE );
                if ( creationDate < minDate || maxDate < creationDate ) return Collections.emptyList();
                return Iterables.filter( endNode.getRelationships( Domain.Rel.HAS_TAG, Direction.OUTGOING ),
                        new Predicate<Relationship>()
                        {
                            @Override
                            public boolean apply( Relationship input )
                            {
                                return input.getEndNode().hasLabel( Domain.Node.TAG );
                            }
                        } );
            }
        };
        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator( Evaluators.atDepth( 3 ) ).expand(
                new StepsExpander( step1, step2, step3 ) );
    }

    /*
    MATCH (person:PERSON)-[:KNOWS*1..2]-(f:PERSON)
     */
    public static TraversalDescription friendsAndFriendsOfFriends()
    {
        Function1<Path, Iterable<Relationship>> step1 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                return from.endNode().getRelationships( Domain.Rel.KNOWS, Direction.BOTH );
            }
        };
        // Note, returns duplicate Nodes
        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator(
                Evaluators.includingDepths( 1, 2 ) ).expand( new StepsExpander( step1, step1 ) );
    }

    /*
        MATCH (friend)<-[:HAS_CREATOR]-(postX:POST)-[:IS_LOCATED_IN]->(countryX:COUNTRY)
        WHERE countryX.name={country_x} AND postX.creationDate>={min_date} AND postX.creationDate<={max_date}
     */
    public static TraversalDescription postsInCountry( final String countryX, final long minDate, final long maxDate )
    {
        Function1<Path, Iterable<Relationship>> step1 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                return from.endNode().getRelationships( Domain.Rel.HAS_CREATOR, Direction.INCOMING );
            }
        };
        Function1<Path, Iterable<Relationship>> step2 = new Function1<Path, Iterable<Relationship>>()
        {
            @Override
            public Iterable<Relationship> apply( Path from )
            {
                Node endNode = from.endNode();
                if ( false == endNode.hasLabel( Domain.Node.POST ) ) return Collections.emptyList();
                long creationDate = (long) endNode.getProperty( Domain.Post.CREATION_DATE );
                if ( creationDate < minDate || maxDate < creationDate ) return Collections.emptyList();
                return Iterables.filter( endNode.getRelationships( Domain.Rel.IS_LOCATED_IN, Direction.OUTGOING ),
                        new Predicate<Relationship>()
                        {
                            @Override
                            public boolean apply( Relationship input )
                            {
                                Node endNode = input.getEndNode();
                                if ( false == endNode.hasLabel( Domain.Place.Type.COUNTRY ) ) return false;
                                String countryName = (String) endNode.getProperty( Domain.Place.NAME );
                                return countryName.equals( countryX );
                            }
                        } );
            }
        };
        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator( Evaluators.atDepth( 2 ) ).expand(
                new StepsExpander( step1, step2 ) );
    }
}
