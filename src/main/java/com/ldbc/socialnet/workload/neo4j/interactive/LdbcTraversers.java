package com.ldbc.socialnet.workload.neo4j.interactive;

import com.google.common.collect.Sets;
import com.ldbc.socialnet.workload.neo4j.Domain;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.traversal.steps.PropertyContainerFilterDescriptor;
import org.neo4j.traversal.steps.Step;
import org.neo4j.traversal.steps.StepsBuilder;

import java.util.Set;

import static org.neo4j.traversal.steps.Filters.node;
import static org.neo4j.traversal.steps.Filters.relationship;

public class LdbcTraversers {
    private final StepsBuilder stepsBuilder;
    private final TraversalDescription baseTraversalDescription;

    public LdbcTraversers(GraphDatabaseService db) {
        this.stepsBuilder = new StepsBuilder();
        this.baseTraversalDescription = db.traversalDescription().uniqueness(Uniqueness.NONE).breadthFirst();
    }

    // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
    // uni.name, uniCity.name(studyAt.classYear)
    public TraversalDescription personUniversities() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.STUDY_AT).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Organisation.Type.University), relationship().hasType(Domain.Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Place.Type.City)));
    }

    // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
    // company.name, companyCountry.name(worksAt.workFrom)
    public TraversalDescription personCompanies() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.WORKS_AT).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Organisation.Type.Company), relationship().hasType(Domain.Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Place.Type.Country)));
    }

    public TraversalDescription tagsOnPostsCreatedByPersonBetweenDates(final long minDate, final long maxDate) {
        PropertyContainerFilterDescriptor.PropertyContainerPredicate creationDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long creationDate = (long) container.getProperty(Domain.Message.CREATION_DATE);
                return (creationDate >= minDate && maxDate > creationDate);
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.KNOWS)),
                Step.one(node().hasLabel(Domain.Nodes.Person), relationship().hasType(Domain.Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Domain.Nodes.Post).conformsTo(creationDateCheck), relationship().hasType(Domain.Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Nodes.Tag)));
    }

    public TraversalDescription friendsPostsAndCommentsBeforeDate(final long maxPostCreationDate) {
        // TODO number range
        PropertyContainerFilterDescriptor.PropertyContainerPredicate creationDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long creationDate = (long) container.getProperty(Domain.Message.CREATION_DATE);
                return (creationDate <= maxPostCreationDate);
            }
        };
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.KNOWS)),
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_CREATOR)),
                Step.one(node().conformsTo(creationDateCheck)));
    }

    public TraversalDescription friendsAndFriendsOfFriends() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.manyRange(node(), relationship().hasType(Domain.Rels.KNOWS), 1, 2));
    }

    public TraversalDescription personsPostsWithGivenTag(final String tagName) {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Domain.Nodes.Post), relationship().hasType(Domain.Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().propertyEquals(Domain.Tag.NAME, tagName)));
    }

    public TraversalDescription tagsOnPosts(final String tagName) {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().propertyNotEquals(Domain.Tag.NAME, tagName)));
    }

    public TraversalDescription postsAndCommentsInCountryInDateRange(final String countryX, final long minDate, final long maxDate) {
        // TODO number range
        PropertyContainerFilterDescriptor.PropertyContainerPredicate creationDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long creationDate = (long) container.getProperty(Domain.Message.CREATION_DATE);
                return (creationDate >= minDate && maxDate >= creationDate);
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().conformsTo(creationDateCheck), relationship().hasType(Domain.Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Place.Type.Country).propertyEquals(Domain.Place.NAME, countryX)));
    }

    public TraversalDescription forumsPersonJoinedAfterDate(final long minDate) {
        // TODO number range
        PropertyContainerFilterDescriptor.PropertyContainerPredicate joinDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long joinDate = (long) container.getProperty(Domain.HasMember.JOIN_DATE);
                return joinDate > minDate;
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_MEMBER).hasDirection(Direction.INCOMING).conformsTo(joinDateCheck)),
                Step.one(node().hasLabel(Domain.Nodes.Forum))
        );
    }

    public TraversalDescription commentsByPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Domain.Nodes.Comment)));
    }

    public TraversalDescription commentsOnPostsInForum(final Set<Node> knownComments) {
        // TODO notHasLabel(Label)
        // TODO notHasProperty(String key)
        // TODO numberPropertyValueInRange(T extends Number in, T extends Number max)
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node().hasLabel(Domain.Nodes.Forum), relationship().hasType(Domain.Rels.CONTAINER_OF).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Nodes.Post), relationship().hasType(Domain.Rels.REPLY_OF).hasDirection(Direction.INCOMING)),
                Step.manyRange(node().hasLabel(Domain.Nodes.Comment).notInSet(knownComments), relationship().hasType(Domain.Rels.REPLY_OF).hasDirection(Direction.INCOMING), 0, Step.UNLIMITED),
                Step.one(node().hasLabel(Domain.Nodes.Comment)));
    }

    public TraversalDescription postsInForumsByPersons(final Set<Node> forums) {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Domain.Nodes.Post), relationship().hasType(Domain.Rels.CONTAINER_OF).hasDirection(Direction.INCOMING)),
                Step.one(node().inSet(forums))
        );
    }

    /*
    (forum)-[:CONTAINER_OF]->(:Post)-[:HAS_CREATOR]->(friend)
     */
    public TraversalDescription postsInForumByFriends(final Set<Node> knownPersons) {
        return stepsBuilder.build(
                baseTraversalDescription,

                Step.one(node().hasLabel(Domain.Nodes.Forum),
                        relationship().hasType(Domain.Rels.CONTAINER_OF).hasDirection(Direction.OUTGOING)),

                Step.one(node().hasLabel(Domain.Nodes.Post),
                        relationship().hasType(Domain.Rels.HAS_CREATOR).hasDirection(Direction.OUTGOING)),

                Step.one(node().inSet(knownPersons)));
    }

    /*
    MATCH (person)-[:IS_LOCATED_IN]->(:City)-[:IS_LOCATED_IN]->(:Country)<-[:IS_LOCATED_IN]-(post:Post)-[:HAS_CREATOR]->(creator:Person),
    WHERE NOT((person)-[:KNOWS]-(creator)) AND post.creationDate>{min_date} AND post.creationDate<{max_date}
     */
    public TraversalDescription postsInPersonsCountryInDateRangeNotCreatedByOtherPerson(final Node otherPerson) {
        // TODO number range
        // TODO remove completely
//        PropertyContainerPredicate creationDateRangeCheck = new PropertyContainerPredicate() {
//            @Override
//            public boolean apply(PropertyContainer container) {
//                long creationDate = (long) ((Node) container).getProperty(Post.CREATION_DATE);
//                return minDate < creationDate && creationDate < maxDate;
//            }
//        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Place.Type.City), relationship().hasType(Domain.Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Place.Type.Country), relationship().hasType(Domain.Rels.IS_LOCATED_IN).hasDirection(Direction.INCOMING)),
                // TODO remove completely
//                Step.one(node().hasLabel(Nodes.Post).conformsTo(creationDateRangeCheck), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Nodes.Person).notInSet(Sets.newHashSet(otherPerson)))
        );
    }

    /*
    MATCH (post)-[:HAS_TAG]->(tag:Tag)
     */
    public TraversalDescription postsTags() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Domain.Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Domain.Nodes.Tag))
        );
    }

}
