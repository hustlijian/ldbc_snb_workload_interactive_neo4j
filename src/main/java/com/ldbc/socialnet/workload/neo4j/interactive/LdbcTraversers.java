package com.ldbc.socialnet.workload.neo4j.interactive;

import com.google.common.collect.Sets;
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

import static com.ldbc.socialnet.workload.neo4j.Domain.*;
import static org.neo4j.traversal.steps.Filters.node;
import static org.neo4j.traversal.steps.Filters.relationship;

public class LdbcTraversers {
    private final StepsBuilder stepsBuilder;
    private final TraversalDescription baseTraversalDescription;

    public LdbcTraversers(GraphDatabaseService db) {
        // TODO (1) for all TraversalDescriptions that don't take a parameter, create only 1 instance
        // TODO (2) add Map<TraverserId, TraversalDescription>
        this.stepsBuilder = new StepsBuilder();
        this.baseTraversalDescription = db.traversalDescription().uniqueness(Uniqueness.NONE).breadthFirst();
    }

    // MATCH (:Person)-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]-(otherPerson:Person)
    // WHERE TODO
    public TraversalDescription commentsMadeByEitherPersonInReplyToCommentsOfOtherPerson(Node otherPerson) {
        Set<Node> otherPersonAsSet = Sets.newHashSet(otherPerson);
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR)),
                Step.one(node().hasLabel(Nodes.Comment), relationship().hasType(Rels.REPLY_OF)),
                Step.one(node().hasLabel(Nodes.Comment), relationship().hasType(Rels.HAS_CREATOR)),
                Step.one(node().inSet(otherPersonAsSet))
        );
    }

    // MATCH (:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->(otherPerson:Person)
    // WHERE TODO
    public TraversalDescription commentsMadeInReplyToPostsOfOtherPerson(Node otherPerson) {
        Set<Node> otherPersonAsSet = Sets.newHashSet(otherPerson);
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasDirection(Direction.INCOMING).hasType(Rels.HAS_CREATOR)),
                Step.one(node().hasLabel(Nodes.Comment), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.REPLY_OF)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.HAS_CREATOR)),
                Step.one(node().inSet(otherPersonAsSet))
        );
    }

    // MATCH (:Person)<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->()-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
    // WHERE TODO
    public TraversalDescription commentsInReplyToPostsTaggedWithTagInGivenTagClassOrDescendentOfThatTagClass(Node tagClass) {
        Set<Node> tagClasses = Sets.newHashSet(tagClass);
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasDirection(Direction.INCOMING).hasType(Rels.HAS_CREATOR)),
                Step.one(node().hasLabel(Nodes.Comment), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.REPLY_OF)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.HAS_TAG)),
                Step.one(node().hasLabel(Nodes.Tag), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.HAS_TYPE)),
                Step.manyRange(node().hasLabel(Nodes.TagClass).notInSet(tagClasses), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.IS_SUBCLASS_OF), 0, Step.UNLIMITED),
                Step.one(node().hasLabel(Nodes.TagClass).inSet(tagClasses))
        );
    }

    // MATCH (:Person)<-[:HAS_CREATOR]-(message)<-[:LIKES]-(:Person)
    public TraversalDescription personsThatLikedMessageCreatedByPerson() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node(), relationship().hasType(Rels.LIKES).hasDirection(Direction.INCOMING)),
                Step.one(node()));
    }

    // MATCH (:Person)-[:STUDY_AT]->(:University)-[:IS_LOCATED_IN]->(:City)
    public TraversalDescription personUniversities() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.STUDY_AT).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Organisation.Type.University), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.City)));
    }

    // MATCH (:Person)-[worksAt:WORKS_AT]->(:Company)-[:IS_LOCATED_IN]->(country:Country)
    // WHERE worksAt.workFrom<maxWorkFromYear AND country.name=countryName
    public TraversalDescription companiesPersonWorkedAtInGivenCountryBeforeGivenDate(String countryName, final int maxWorkFromYear) {
        PropertyContainerFilterDescriptor.PropertyContainerPredicate workFromYearCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                int workFromYear = (int) container.getProperty(WorksAt.WORK_FROM);
                return (workFromYear < maxWorkFromYear);
            }
        };
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.WORKS_AT).hasDirection(Direction.OUTGOING).conformsTo(workFromYearCheck)),
                Step.one(node().hasLabel(Organisation.Type.Company), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.Country).propertyEquals(Place.NAME, countryName)));
    }

    // MATCH (:Person)-[:WORKS_AT]->(:Company)-[:IS_LOCATED_IN]->(:Country)
    public TraversalDescription companiesPersonWorkedAtAndTheCountryEachCompanyIsIn() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.WORKS_AT).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Organisation.Type.Company), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.Country)));
    }

    // MATCH (:Person)-[:WORKS_AT]->(:Company)-[:IS_LOCATED_IN]->(:Country)
    // WHERE TODO
    public TraversalDescription tagsOnPostsCreatedByPersonBetweenDates(final long minDate, final long maxDate) {
        PropertyContainerFilterDescriptor.PropertyContainerPredicate creationDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long creationDate = (long) container.getProperty(Message.CREATION_DATE);
                return (creationDate >= minDate && maxDate > creationDate);
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.KNOWS)),
                Step.one(node().hasLabel(Nodes.Person), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Post).conformsTo(creationDateCheck), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Nodes.Tag)));
    }

    // TODO cypher description
    public TraversalDescription friendsPostsAndCommentsBeforeDate(final long maxPostCreationDate) {
        PropertyContainerFilterDescriptor.PropertyContainerPredicate creationDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long creationDate = (long) container.getProperty(Message.CREATION_DATE);
                return (creationDate <= maxPostCreationDate);
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.KNOWS)),
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR)),
                Step.one(node().conformsTo(creationDateCheck)));
    }

    // TODO cypher description
    public TraversalDescription friendsAndFriendsOfFriends() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.manyRange(node(), relationship().hasType(Rels.KNOWS), 1, 2)
        );
    }

    // TODO cypher description
    public TraversalDescription friendsOfFriends() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.manyRange(node(), relationship().hasType(Rels.KNOWS), 2, 2)
        );
    }

    // TODO cypher description
    public TraversalDescription personsPostsWithGivenTag(final String tagName) {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().propertyEquals(Tag.NAME, tagName)));
    }

    // TODO cypher description
    public TraversalDescription tagsOnPostsExcludingGivenTag(final String tagName) {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().propertyNotEquals(Tag.NAME, tagName)));
    }

    // TODO cypher description
    public TraversalDescription postsAndCommentsInCountryInDateRange(final String countryX, final long minDate, final long maxDate) {
        PropertyContainerFilterDescriptor.PropertyContainerPredicate creationDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long creationDate = (long) container.getProperty(Message.CREATION_DATE);
                return (creationDate >= minDate && maxDate >= creationDate);
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().conformsTo(creationDateCheck), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.Country).propertyEquals(Place.NAME, countryX)));
    }

    // TODO cypher description
    public TraversalDescription forumsPersonJoinedAfterDate(final long minDate) {
        // TODO add number range to Steps
        PropertyContainerFilterDescriptor.PropertyContainerPredicate joinDateCheck = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long joinDate = (long) container.getProperty(HasMember.JOIN_DATE);
                return joinDate > minDate;
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_MEMBER).hasDirection(Direction.INCOMING).conformsTo(joinDateCheck)),
                Step.one(node().hasLabel(Nodes.Forum))
        );
    }

    // TODO cypher description
    public TraversalDescription postsByPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Post))
        );
    }

    // TODO cypher description
    public TraversalDescription commentsAndPostsByPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node())
        );
    }

    // TODO cypher description
    public TraversalDescription commentsAndPostsByPersonCreatedBeforeDate(final long date) {
        PropertyContainerFilterDescriptor.PropertyContainerPredicate createdBeforeDate = new PropertyContainerFilterDescriptor.PropertyContainerPredicate() {
            @Override
            public boolean apply(PropertyContainer container) {
                long creationDate = (long) container.getProperty(Message.CREATION_DATE);
                return creationDate < date;
            }
        };
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().conformsTo(createdBeforeDate))
        );
    }

    // TODO cypher description
    public TraversalDescription commentsRepliedToPostOrCommentExcludingThoseByGivenPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.manyRange(node(), relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING), 1, Step.UNLIMITED)
        );
    }

    // TODO cypher description
    public TraversalDescription postsInForumsByPersons(final Set<Node> forums) {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasType(Rels.CONTAINER_OF).hasDirection(Direction.INCOMING)),
                Step.one(node().inSet(forums))
        );
    }
}
