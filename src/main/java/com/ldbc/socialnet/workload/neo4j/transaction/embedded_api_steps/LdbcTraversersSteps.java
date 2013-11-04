package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps;

import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.traversal.steps.Step;
import org.neo4j.traversal.steps.StepsBuilder;
import org.neo4j.traversal.steps.PropertyContainerFilterDescriptor.PropertyContainerPredicate;

import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;

import static org.neo4j.traversal.steps.Filters.*;

public class LdbcTraversersSteps implements LdbcTraversers
{
    private final StepsBuilder stepsBuilder;
    private final TraversalDescription baseTraversalDescription;

    public LdbcTraversersSteps( GraphDatabaseService db )
    {
        this.stepsBuilder = new StepsBuilder();
        this.baseTraversalDescription = db.traversalDescription().uniqueness( Uniqueness.NONE ).breadthFirst();
    }

    // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
    // uni.name, uniCity.name(studyAt.classYear)
    @Override
    public TraversalDescription personUniversities()
    {
        return stepsBuilder.build(
                baseTraversalDescription,

                Step.one( node(), relationship().hasType( Domain.Rel.STUDY_AT ).hasDirection( Direction.OUTGOING ) ),

                Step.one( node().hasLabel( Domain.Organisation.Type.University ),
                        relationship().hasType( Domain.Rel.IS_LOCATED_IN ).hasDirection( Direction.OUTGOING ) ),

                Step.one( node().hasLabel( Domain.Place.Type.City ) ) );
    }

    // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
    // company.name, companyCountry.name(worksAt.workFrom)
    @Override
    public TraversalDescription personCompanies()
    {
        return stepsBuilder.build(
                baseTraversalDescription,

                Step.one( node(), relationship().hasType( Domain.Rel.WORKS_AT ).hasDirection( Direction.OUTGOING ) ),

                Step.one( node().hasLabel( Domain.Organisation.Type.Company ),
                        relationship().hasType( Domain.Rel.IS_LOCATED_IN ).hasDirection( Direction.OUTGOING ) ),

                Step.one( node().hasLabel( Domain.Place.Type.Country ) ) );
    }

    /*
        MATCH (person:PERSON)-[:KNOWS]-(friend:PERSON)
        MATCH (friend)<-[:HAS_CREATOR]-(post:POST)
        WHERE post.creationDate>={min_date} AND post.creationDate<={max_date}
        MATCH (post)-[HAS_TAG]->(tag:TAG)
     */
    @Override
    public TraversalDescription friendPostTags( final long minDate, final long maxDate )
    {
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
        return stepsBuilder.build( baseTraversalDescription,

        Step.one( node(), relationship().hasType( Domain.Rel.KNOWS ) ),

        Step.one( node().hasLabel( Domain.Node.Person ),
                relationship().hasType( Domain.Rel.HAS_CREATOR ).hasDirection( Direction.INCOMING ) ),

        Step.one( node().hasLabel( Domain.Node.Post ).conformsTo( creationDateCheck ),
                relationship().hasType( Domain.Rel.HAS_TAG ).hasDirection( Direction.OUTGOING ) ),

        Step.one( node().hasLabel( Domain.Node.Tag ) ) );
    }

    /*
    MATCH (person:PERSON)-[:KNOWS*1..2]-(f:PERSON)
     */
    @Override
    public TraversalDescription friendsAndFriendsOfFriends()
    {
        return stepsBuilder.build( baseTraversalDescription,

        Step.manyRange( node(), relationship().hasType( Domain.Rel.KNOWS ), 1, 2 ) );
    }

    /*
        MATCH (friend)<-[:HAS_CREATOR]-(postX:POST)-[:IS_LOCATED_IN]->(countryX:COUNTRY)
        WHERE countryX.name={country_x} AND postX.creationDate>={min_date} AND postX.creationDate<={max_date}
     */
    @Override
    public TraversalDescription postsInCountry( final String countryX, final long minDate, final long maxDate )
    {
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
        return stepsBuilder.build(
                baseTraversalDescription,

                Step.one( node(), relationship().hasType( Domain.Rel.HAS_CREATOR ).hasDirection( Direction.INCOMING ) ),

                Step.one( node().hasLabel( Domain.Node.Post ).conformsTo( creationDateCheck ),
                        relationship().hasType( Domain.Rel.IS_LOCATED_IN ).hasDirection( Direction.OUTGOING ) ),

                Step.one( node().hasLabel( Domain.Place.Type.Country ).propertyEquals( Domain.Place.NAME, countryX ) ) );
    }

    /*
    MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
    WHERE membership.joinDate>{join_date}
     */
    @Override
    public TraversalDescription personsMembershipForums( final long minDate )
    {
        // TODO number range
        PropertyContainerPredicate joinDateCheck = new PropertyContainerPredicate()
        {
            @Override
            public boolean apply( PropertyContainer container )
            {
                long joinDate = (long) ( (Relationship) container ).getProperty( Domain.HasMember.JOIN_DATE );
                return joinDate > minDate;
            }
        };
        return stepsBuilder.build( baseTraversalDescription,

        Step.one(
                node(),
                relationship().hasType( Domain.Rel.HAS_MEMBER ).hasDirection( Direction.INCOMING ).conformsTo(
                        joinDateCheck ) ),

        Step.one( node().hasLabel( Domain.Node.Forum ) ) );
    }

    /*
    MATCH (friend)<-[:HAS_CREATOR]-(comment:Comment)
     */
    @Override
    public TraversalDescription personsComments()
    {
        return stepsBuilder.build( baseTraversalDescription,

        Step.one( node(), relationship().hasType( Domain.Rel.HAS_CREATOR ).hasDirection( Direction.INCOMING ) ),

        Step.one( node().hasLabel( Domain.Node.Comment ) ) );
    }

    /*    
    (forum)-[:CONTAINER_OF]->(:Post)<-[:REPLY_OF]-(:Comment)<-[:REPLY_OF*]-(comment)    
    traverse from forum, find all comments that are also in knownComments
     */
    @Override
    public TraversalDescription commentsOnPostsInForum( final Set<Node> knownComments )
    {
        // TODO notHasLabel(Label)
        // TODO notHasProperty(String key)
        // TODO numberPropertyValueInRange(T extends Number in, T extends Number
        // max)

        return stepsBuilder.build( baseTraversalDescription,

        Step.one( node().hasLabel( Domain.Node.Forum ),
                relationship().hasType( Domain.Rel.CONTAINER_OF ).hasDirection( Direction.OUTGOING ) ),

        Step.one( node().hasLabel( Domain.Node.Post ),
                relationship().hasType( Domain.Rel.REPLY_OF ).hasDirection( Direction.INCOMING ) ),

        Step.manyRange( node().hasLabel( Domain.Node.Comment ).notInSet( knownComments ),
                relationship().hasType( Domain.Rel.REPLY_OF ).hasDirection( Direction.INCOMING ), 0, Step.UNLIMITED ),

        Step.one( node().hasLabel( Domain.Node.Comment ).inSet( knownComments ) ) );
    }

    /*
    (forum)-[:CONTAINER_OF]->(:Post)-[:HAS_CREATOR]->(friend)
     */
    @Override
    public TraversalDescription postsInForumByFriends( final Set<Node> knownPersons )
    {
        return stepsBuilder.build( baseTraversalDescription,

        Step.one( node().hasLabel( Domain.Node.Forum ),
                relationship().hasType( Domain.Rel.CONTAINER_OF ).hasDirection( Direction.OUTGOING ) ),

        Step.one( node().hasLabel( Domain.Node.Post ),
                relationship().hasType( Domain.Rel.HAS_CREATOR ).hasDirection( Direction.OUTGOING ) ),

        Step.one( node().inSet( knownPersons ) ) );
    }
}
