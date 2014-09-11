package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery10;
import org.neo4j.graphdb.*;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.*;

public class Neo4jQuery10EmbeddedApi extends Neo4jQuery10<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery10EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query10 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery10Result> execute(GraphDatabaseService db, final LdbcQuery10 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        Iterator<Node> friendsAndFriendsOfFriends = Iterators.transform(
                traversers.friendsOfFriends().traverse(person).iterator(),
                new Function<Path, Node>() {
                    @Override
                    public Node apply(Path path) {
                        return path.endNode();
                    }
                }
        );

        final int nextMonth = (operation.month() + 1) % 12;
        Iterator<Node> friendsOfFriendsWithMatchingBirthdays = StepsUtils.distinct(
                Iterators.filter(
                        friendsAndFriendsOfFriends,
                        new Predicate<Node>() {
                            @Override
                            public boolean apply(Node friendOfFriend) {
                                if (person.equals(friendOfFriend)) return false;
                                if (personKnowsPerson(person, friendOfFriend)) return false;
                                int birthdayMonth = (int) friendOfFriend.getProperty(Domain.Person.BIRTHDAY_MONTH);
                                int birthdayDayOfMonth = (int) friendOfFriend.getProperty(Domain.Person.BIRTHDAY_DAY_OF_MONTH);
                                if (birthdayMonth == operation.month()) {
                                    return birthdayDayOfMonth >= 21;
                                } else if (birthdayMonth == nextMonth) {
                                    return birthdayDayOfMonth < 22;
                                } else {
                                    return false;
                                }
                            }
                        }
                )
        );

        final Set<Node> tagsPersonIsInterestedIn = new HashSet<>();
        for (Relationship relationship : person.getRelationships(Domain.Rels.HAS_INTEREST, Direction.OUTGOING)) {
            Node tag = relationship.getEndNode();
            tagsPersonIsInterestedIn.add(tag);
        }

        List<LdbcQuery10Result> results = Lists.newArrayList(
                Iterators.transform(
                        friendsOfFriendsWithMatchingBirthdays,
                        new Function<Node, LdbcQuery10Result>() {
                            @Override
                            public LdbcQuery10Result apply(Node friendOfFriend) {
                                int commonInterestScore = 0;
                                for (Path path : traversers.postsByPerson().traverse(friendOfFriend)) {
                                    Node post = path.endNode();
                                    if (postIsTaggedWithAnyOfGivenTags(post, tagsPersonIsInterestedIn))
                                        commonInterestScore++;
                                    else
                                        commonInterestScore--;
                                }
                                long personId = (long) friendOfFriend.getProperty(Domain.Person.ID);
                                String personFirstName = (String) friendOfFriend.getProperty(Domain.Person.FIRST_NAME);
                                String personLastName = (String) friendOfFriend.getProperty(Domain.Person.LAST_NAME);
                                String personGender = (String) friendOfFriend.getProperty(Domain.Person.GENDER);
                                String personCityName = (String) friendOfFriend.getRelationships(Domain.Rels.IS_LOCATED_IN).iterator().next().getEndNode().getProperty(Domain.Place.NAME);
                                return new LdbcQuery10Result(personId, personFirstName, personLastName, commonInterestScore, personGender, personCityName);
                            }
                        }
                )
        );

        Collections.sort(results, new DescendingSimilarityScoreAscendingPersonId());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    private boolean postIsTaggedWithAnyOfGivenTags(Node post, Set<Node> tags) {
        for (Relationship relationship : post.getRelationships(Domain.Rels.HAS_TAG, Direction.OUTGOING)) {
            Node postTag = relationship.getEndNode();
            if (tags.contains(postTag)) return true;
        }
        return false;
    }

    private boolean personKnowsPerson(Node person1, Node person2) {
        for (Relationship knowsRelationship : person1.getRelationships(Domain.Rels.KNOWS)) {
            Node otherPerson = knowsRelationship.getOtherNode(person1);
            if (otherPerson.equals(person2)) return true;
        }
        return false;
    }

    public static class DescendingSimilarityScoreAscendingPersonId implements Comparator<LdbcQuery10Result> {
        @Override
        public int compare(LdbcQuery10Result result1, LdbcQuery10Result result2) {
            if (result1.commonInterestScore() > result2.commonInterestScore()) return -1;
            else if (result1.commonInterestScore() < result2.commonInterestScore()) return 1;
            else {
                if (result1.personId() < result2.personId()) return -1;
                else if (result1.personId() > result2.personId()) return 1;
                else return 0;
            }
        }
    }
}
