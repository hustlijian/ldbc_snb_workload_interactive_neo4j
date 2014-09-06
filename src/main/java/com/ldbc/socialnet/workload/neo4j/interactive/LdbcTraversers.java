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
        this.stepsBuilder = new StepsBuilder();
        this.baseTraversalDescription = db.traversalDescription().uniqueness(Uniqueness.NONE).breadthFirst();
    }

    // (friend)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF*]->()-[:HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
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

    // (tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
    public TraversalDescription tagsInGivenTagClassOrDescendentOfThatTagClass1(String tagClassName) {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node().hasLabel(Nodes.Tag), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.HAS_TYPE)),
                Step.manyRange(node().hasLabel(Nodes.TagClass).propertyNotEquals(TagClass.NAME, tagClassName), relationship().hasDirection(Direction.OUTGOING).hasType(Rels.IS_SUBCLASS_OF), 0, Step.UNLIMITED),
                Step.one(node().hasLabel(Nodes.TagClass).propertyEquals(TagClass.NAME, tagClassName))
        );
    }

    // (baseTagClass:TagClass)<-[:IS_SUBCLASS_OF*0..]-(tagClass:TagClass)<-[:HAS_TYPE]-(tag:Tag)
    public TraversalDescription tagsInGivenTagClassOrDescendentOfThatTagClass2() {
        return stepsBuilder.build(baseTraversalDescription,
//                Step.manyRange(node().hasLabel(Nodes.TagClass), relationship().hasDirection(Direction.INCOMING).hasType(Rels.IS_SUBCLASS_OF), 0, Step.UNLIMITED),
//                Step.one(node().hasLabel(Nodes.TagClass), relationship().hasDirection(Direction.INCOMING).hasType(Rels.HAS_TYPE)),
//                Step.one(node().hasLabel(Nodes.Tag))
                Step.manyRange(node().hasLabel(Nodes.TagClass), relationship().hasDirection(Direction.INCOMING).hasType(Rels.IS_SUBCLASS_OF), 0, Step.UNLIMITED),
                Step.one(node().hasLabel(Nodes.TagClass))
        );
    }

    public TraversalDescription personsThatLikedMessageCreatedByPerson() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node(), relationship().hasType(Rels.LIKES).hasDirection(Direction.INCOMING)),
                Step.one(node()));
    }

    public TraversalDescription personUniversities() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.STUDY_AT).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Organisation.Type.University), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.City)));
    }

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

    public TraversalDescription companiesPersonWorkedAtAndTheCountryEachCompanyIsIn() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.WORKS_AT).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Organisation.Type.Company), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.Country)));
    }

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

    public TraversalDescription friendsPostsAndCommentsBeforeDate(final long maxPostCreationDate) {
        // TODO number range
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

    public TraversalDescription friendsAndFriendsOfFriends() {
        return stepsBuilder.build(baseTraversalDescription,
                Step.manyRange(node(), relationship().hasType(Rels.KNOWS), 1, 2)
        );
    }

    public TraversalDescription friendsOfFriends() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.manyRange(node(), relationship().hasType(Rels.KNOWS), 2, 2)
        );
    }

    public TraversalDescription personsPostsWithGivenTag(final String tagName) {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().propertyEquals(Tag.NAME, tagName)));
    }

    public TraversalDescription tagsOnPostsExcludingGivenTag(final String tagName) {
        return stepsBuilder.build(baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().propertyNotEquals(Tag.NAME, tagName)));
    }

    public TraversalDescription postsAndCommentsInCountryInDateRange(final String countryX, final long minDate, final long maxDate) {
        // TODO number range
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

    public TraversalDescription forumsPersonJoinedAfterDate(final long minDate) {
        // TODO number range
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

    public TraversalDescription commentsByPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Comment))
        );
    }

    public TraversalDescription postsByPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Post))
        );
    }

    public TraversalDescription commentsAndPostsByPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node())
        );
    }

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

    public TraversalDescription commentsRepliedToPostOrCommentExcludingThoseByGivenPerson() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.manyRange(node(), relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING), 1, Step.UNLIMITED)
        );
    }

    /*
MATCH (start:Person {id:{1}})<-[:HAS_CREATOR]-(message)<-[:REPLY_OF*]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
WHERE not(start=person)
RETURN DISTINCT person.id AS personId, person.firstName AS personFirstName, person.lastName AS personLastName, comment.id AS commentId, comment.creationDate AS commentCreationDate, comment.content AS commentContent
ORDER BY commentCreationDate DESC, commentId ASC
LIMIT {2}
 */
    public TraversalDescription commentsByOtherPersonsThatWereRepliesToStartPersonsCommentsOrPosts(final Node startPerson) {
        return stepsBuilder.build(
                baseTraversalDescription,
                // (:Person)<-[:HAS_CREATOR]-
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                // (message)<-[:REPLY_OF]-
                Step.one(node(), relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING)),
//                // (:Comment)<-[:REPLY_OF]-
//                Step.one(node().hasLabel(Nodes.Comment), relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING)),
                // (:Comment)<-[:REPLY_OF*0..]-
                Step.manyRange(node().hasLabel(Nodes.Comment), relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING), 0, 3),
                // (:Comment)-[:HAS_CREATOR]->
                Step.one(node().hasLabel(Nodes.Comment), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.OUTGOING)),
                // (:Person)
                Step.one(node().notInSet(Sets.newHashSet(startPerson)))
        );
    }


    public TraversalDescription commentsOnPostsInForum(final Set<Node> knownComments) {
        // TODO notHasLabel(Label)
        // TODO notHasProperty(String key)
        // TODO numberPropertyValueInRange(T extends Number in, T extends Number max)
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node().hasLabel(Nodes.Forum), relationship().hasType(Rels.CONTAINER_OF).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING)),
                Step.manyRange(node().hasLabel(Nodes.Comment).notInSet(knownComments), relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING), 0, Step.UNLIMITED),
                Step.one(node().hasLabel(Nodes.Comment)));
    }

    public TraversalDescription postsInForumsByPersons(final Set<Node> forums) {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasType(Rels.CONTAINER_OF).hasDirection(Direction.INCOMING)),
                Step.one(node().inSet(forums))
        );
    }

    public TraversalDescription postsInForumByFriends(final Set<Node> knownPersons) {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node().hasLabel(Nodes.Forum), relationship().hasType(Rels.CONTAINER_OF).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Nodes.Post), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.OUTGOING)),
                Step.one(node().inSet(knownPersons)));
    }

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
                Step.one(node(), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.City), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Place.Type.Country), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.INCOMING)),
                // TODO remove completely
//                Step.one(node().hasLabel(Nodes.Post).conformsTo(creationDateRangeCheck), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Nodes.Person).notInSet(Sets.newHashSet(otherPerson)))
        );
    }

    public TraversalDescription postsTags() {
        return stepsBuilder.build(
                baseTraversalDescription,
                Step.one(node(), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
                Step.one(node().hasLabel(Nodes.Tag))
        );
    }

}
