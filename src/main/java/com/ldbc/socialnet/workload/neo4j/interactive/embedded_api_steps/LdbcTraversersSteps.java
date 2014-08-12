package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import java.util.Set;

public class LdbcTraversersSteps implements LdbcTraversers {
    // TODO uncomment
//    private final StepsBuilder stepsBuilder;
    private final TraversalDescription baseTraversalDescription;

    public LdbcTraversersSteps(GraphDatabaseService db) {
        // TODO uncomment
//        this.stepsBuilder = new StepsBuilder();
        this.baseTraversalDescription = db.traversalDescription().uniqueness(Uniqueness.NONE).breadthFirst();
    }

    // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
    // uni.name, uniCity.name(studyAt.classYear)
    @Override
    public TraversalDescription personUniversities() {
        // TODO uncomment
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.STUDY_AT).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Organisation.Type.University),
//                        relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Place.Type.City)));
        // TODO remove
        return null;
    }

    // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
    // company.name, companyCountry.name(worksAt.workFrom)
    @Override
    public TraversalDescription personCompanies() {
        // TODO uncomment
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.WORKS_AT).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Organisation.Type.Company),
//                        relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Place.Type.Country)));
        // TODO remove
        return null;
    }

    /*
        MATCH (person:PERSON)-[:KNOWS]-(friend:PERSON)
        MATCH (friend)<-[:HAS_CREATOR]-(post:POST)
        WHERE post.creationDate>={min_date} AND post.creationDate<={max_date}
        MATCH (post)-[HAS_TAG]->(tag:TAG)
     */
    @Override
    public TraversalDescription friendPostTags(final long minDate, final long maxDate) {
        // TODO uncomment
//        // TODO number range
//        PropertyContainerPredicate creationDateCheck = new PropertyContainerPredicate() {
//            @Override
//            public boolean apply(PropertyContainer container) {
//                long creationDate = (long) ((Node) container).getProperty(Post.CREATION_DATE);
//                return (creationDate >= minDate && maxDate >= creationDate);
//            }
//        };
//        return stepsBuilder.build(
//                baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.KNOWS)),
//
//                Step.one(node().hasLabel(Nodes.Person),
//                        relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
//
//                Step.one(node().hasLabel(Nodes.Post).conformsTo(creationDateCheck),
//                        relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Nodes.Tag)));
        // TODO remove
        return null;
    }

    /*
    MATCH (:Person {id:{person_id}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)
    WHERE post.creationDate<={max_date}
    LIMIT 20
     */
    @Override
    public TraversalDescription friendsPostsBeforeDate(final long maxPostCreationDate) {
        // TODO uncomment
//        // TODO number range
//        PropertyContainerPredicate creationDateCheck = new PropertyContainerPredicate() {
//            @Override
//            public boolean apply(PropertyContainer container) {
//                long creationDate = (long) ((Node) container).getProperty(Post.CREATION_DATE);
//                return (creationDate <= maxPostCreationDate);
//            }
//        };
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.KNOWS)),
//
//                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR)),
//
//                Step.one(node().hasLabel(Nodes.Post).conformsTo(creationDateCheck)));
        // TODO remove
        return null;
    }

    /*
    MATCH (person:PERSON)-[:KNOWS*1..2]-(friend:PERSON)
     */
    @Override
    public TraversalDescription friendsAndFriendsOfFriends() {
        // TODO uncomment
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.manyRange(node(), relationship().hasType(Rels.KNOWS), 1, 2));
        // TODO remove
        return null;
    }

    /*
    MATCH (person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag {name:{tag_name}})
     */
    @Override
    public TraversalDescription personsPostsWithGivenTag(final String tagName) {
        // TODO uncomment
//        return stepsBuilder.build(
//                baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
//
//                Step.one(node().hasLabel(Nodes.Post),
//                        relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().propertyEquals(Tag.NAME, tagName)));
        // TODO remove
        return null;
    }

    /*
    MATCH (post)-[:HAS_TAG]->(tag:Tag)
    WHERE NOT(tag.name={tag_name})
     */
    @Override
    public TraversalDescription tagsOnPosts(final String tagName) {
        // TODO uncomment
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().propertyNotEquals(Tag.NAME, tagName)));
        // TODO remove
        return null;
    }

    /*
        MATCH (friend)<-[:HAS_CREATOR]-(postX:POST)-[:IS_LOCATED_IN]->(countryX:COUNTRY)
        WHERE countryX.name={country_x} AND postX.creationDate>={min_date} AND postX.creationDate<={max_date}
     */
    @Override
    public TraversalDescription postsInCountry(final String countryX, final long minDate, final long maxDate) {
        // TODO uncomment
//        // TODO number range
//        PropertyContainerPredicate creationDateCheck = new PropertyContainerPredicate() {
//            @Override
//            public boolean apply(PropertyContainer container) {
//                long creationDate = (long) ((Node) container).getProperty(Post.CREATION_DATE);
//                return (creationDate >= minDate && maxDate >= creationDate);
//            }
//        };
//        return stepsBuilder.build(
//                baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
//
//                Step.one(node().hasLabel(Nodes.Post).conformsTo(creationDateCheck),
//                        relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Place.Type.Country).propertyEquals(Place.NAME, countryX)));
        // TODO remove
        return null;
    }

    /*
    MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
    WHERE membership.joinDate>{join_date}
     */
    @Override
    public TraversalDescription personsMembershipForums(final long minDate) {
        // TODO uncomment
//        // TODO number range
//        PropertyContainerPredicate joinDateCheck = new PropertyContainerPredicate() {
//            @Override
//            public boolean apply(PropertyContainer container) {
//                long joinDate = (long) ((Relationship) container).getProperty(HasMember.JOIN_DATE);
//                return joinDate > minDate;
//            }
//        };
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(
//                        node(),
//                        relationship().hasType(Rels.HAS_MEMBER).hasDirection(Direction.INCOMING).conformsTo(
//                                joinDateCheck)),
//
//                Step.one(node().hasLabel(Nodes.Forum)));
        // TODO remove
        return null;
    }

    /*
    MATCH (friend)<-[:HAS_CREATOR]-(comment:Comment)
     */
    @Override
    public TraversalDescription personsComments() {
        // TODO uncomment
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.INCOMING)),
//
//                Step.one(node().hasLabel(Nodes.Comment)));
        // TODO remove
        return null;
    }

    /*    
    (forum)-[:CONTAINER_OF]->(:Post)<-[:REPLY_OF]-(:Comment)<-[:REPLY_OF*]-(comment)    
    traverse from forum, find all comments that are also in knownComments
     */
    @Override
    public TraversalDescription commentsOnPostsInForum(final Set<Node> knownComments) {
        // TODO uncomment
//        // TODO notHasLabel(Label)
//        // TODO notHasProperty(String key)
//        // TODO numberPropertyValueInRange(T extends Number in, T extends Number
//        // max)
//
//        return stepsBuilder.build(
//                baseTraversalDescription,
//
//                Step.one(node().hasLabel(Nodes.Forum),
//                        relationship().hasType(Rels.CONTAINER_OF).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Nodes.Post),
//                        relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING)),
//
//                Step.manyRange(node().hasLabel(Nodes.Comment).notInSet(knownComments),
//                        relationship().hasType(Rels.REPLY_OF).hasDirection(Direction.INCOMING), 0, Step.UNLIMITED),
//
//                Step.one(node().hasLabel(Nodes.Comment)));
        // TODO remove
        return null;
    }

    /*
    (forum)-[:CONTAINER_OF]->(:Post)-[:HAS_CREATOR]->(friend)
     */
    @Override
    public TraversalDescription postsInForumByFriends(final Set<Node> knownPersons) {
        // TODO uncomment
//        return stepsBuilder.build(
//                baseTraversalDescription,
//
//                Step.one(node().hasLabel(Nodes.Forum),
//                        relationship().hasType(Rels.CONTAINER_OF).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Nodes.Post),
//                        relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().inSet(knownPersons)));
        // TODO remove
        return null;
    }

    /*
    MATCH (person)-[:IS_LOCATED_IN]->(:City)-[:IS_LOCATED_IN]->(:Country)<-[:IS_LOCATED_IN]-(post:Post)-[:HAS_CREATOR]->(creator:Person),
    WHERE NOT((person)-[:KNOWS]-(creator)) AND post.creationDate>{min_date} AND post.creationDate<{max_date}
     */
    @Override
    public TraversalDescription postsInPersonsCountryInDateRangeNotCreatedByOtherPerson(final Node otherPerson) {
        // TODO uncomment
//        // TODO number range
//        // TODO remove completely
////        PropertyContainerPredicate creationDateRangeCheck = new PropertyContainerPredicate() {
////            @Override
////            public boolean apply(PropertyContainer container) {
////                long creationDate = (long) ((Node) container).getProperty(Post.CREATION_DATE);
////                return minDate < creationDate && creationDate < maxDate;
////            }
////        };
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Place.Type.City),
//                        relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Place.Type.Country),
//                        relationship().hasType(Rels.IS_LOCATED_IN).hasDirection(Direction.INCOMING)),
//
//                // TODO remove completely
////                Step.one(node().hasLabel(Nodes.Post).conformsTo(creationDateRangeCheck),
////                        relationship().hasType(Rels.HAS_CREATOR).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Nodes.Person).notInSet(Sets.newHashSet(otherPerson))));
        // TODO remove
        return null;
    }

    /*
    MATCH (post)-[:HAS_TAG]->(tag:Tag)
     */
    @Override
    public TraversalDescription postsTags() {
        // TODO uncomment
//        return stepsBuilder.build(baseTraversalDescription,
//
//                Step.one(node(), relationship().hasType(Rels.HAS_TAG).hasDirection(Direction.OUTGOING)),
//
//                Step.one(node().hasLabel(Nodes.Tag)));
        // TODO remove
        return null;
    }

}
