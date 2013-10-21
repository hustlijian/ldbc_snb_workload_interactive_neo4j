package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.neo4j.traversal.Filters;
import com.ldbc.socialnet.workload.neo4j.traversal.PropertyContainerFilterDescriptor.PropertyContainerPredicate;
import com.ldbc.socialnet.workload.neo4j.traversal.Step;
import com.ldbc.socialnet.workload.neo4j.traversal.StepsExpander;

public class LdbcTraversers
{
    // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
    // uni.name, uniCity.name(studyAt.classYear)
    public static TraversalDescription personUniversities()
    {
        Step step1 = new Step( Filters.node(), Filters.relationship().hasType( Domain.Rel.STUDY_AT ).hasDirection(
                Direction.OUTGOING ), Filters.node() );

        Step step2 = new Step( Filters.node().hasLabel( Domain.Organisation.Type.University ),
                Filters.relationship().hasType( Domain.Rel.IS_LOCATED_IN ).hasDirection( Direction.OUTGOING ),
                Filters.node().hasLabel( Domain.Place.Type.City ) );

        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator( Evaluators.atDepth( 2 ) ).expand(
                new StepsExpander( step1, step2 ) );
    }

    // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
    // company.name, companyCountry.name(worksAt.workFrom)
    public static TraversalDescription personCompanies()
    {
        Step step1 = new Step( Filters.node(), Filters.relationship().hasType( Domain.Rel.WORKS_AT ).hasDirection(
                Direction.OUTGOING ), Filters.node() );

        Step step2 = new Step( Filters.node().hasLabel( Domain.Organisation.Type.Company ),
                Filters.relationship().hasType( Domain.Rel.IS_LOCATED_IN ).hasDirection( Direction.OUTGOING ),
                Filters.node().hasLabel( Domain.Place.Type.Country ) );

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
        Step step1 = new Step( Filters.node(), Filters.relationship().hasType( Domain.Rel.KNOWS ), Filters.node() );

        Step step2 = new Step( Filters.node().hasLabel( Domain.Node.Person ), Filters.relationship().hasType(
                Domain.Rel.HAS_CREATOR ).hasDirection( Direction.INCOMING ), Filters.node() );

        // TODO number range
        PropertyContainerPredicate creationDateCheck = new PropertyContainerPredicate()
        {
            @Override
            public boolean apply( PropertyContainer container )
            {
                long creationDate = (long) ( (Node) container ).getProperty( Domain.Post.CREATION_DATE );
                return ( creationDate >= minDate && maxDate >= creationDate );
            }
        };
        Step step3 = new Step( Filters.node().hasLabel( Domain.Node.Post ).conformsTo( creationDateCheck ),
                Filters.relationship().hasType( Domain.Rel.HAS_TAG ).hasDirection( Direction.OUTGOING ),
                Filters.node().hasLabel( Domain.Node.Tag ) );

        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator( Evaluators.atDepth( 3 ) ).expand(
                new StepsExpander( step1, step2, step3 ) );
    }

    /*
    MATCH (person:PERSON)-[:KNOWS*1..2]-(f:PERSON)
     */
    public static TraversalDescription friendsAndFriendsOfFriends()
    {
        Step step1 = new Step( Filters.node(), Filters.relationship().hasType( Domain.Rel.KNOWS ), Filters.node() );

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
        Step step1 = new Step( Filters.node(), Filters.relationship().hasType( Domain.Rel.HAS_CREATOR ).hasDirection(
                Direction.INCOMING ), Filters.node() );

        // TODO number range
        PropertyContainerPredicate creationDateCheck = new PropertyContainerPredicate()
        {
            @Override
            public boolean apply( PropertyContainer container )
            {
                long creationDate = (long) ( (Node) container ).getProperty( Domain.Post.CREATION_DATE );
                return ( creationDate >= minDate && maxDate >= creationDate );
            }
        };
        Step step2 = new Step( Filters.node().hasLabel( Domain.Node.Post ).conformsTo( creationDateCheck ),
                Filters.relationship().hasType( Domain.Rel.IS_LOCATED_IN ).hasDirection( Direction.OUTGOING ),
                Filters.node().hasLabel( Domain.Place.Type.Country ).propertyEquals( Domain.Place.NAME, countryX ) );

        return Traversal.description().uniqueness( Uniqueness.NONE ).breadthFirst().evaluator( Evaluators.atDepth( 2 ) ).expand(
                new StepsExpander( step1, step2 ) );
    }
}
