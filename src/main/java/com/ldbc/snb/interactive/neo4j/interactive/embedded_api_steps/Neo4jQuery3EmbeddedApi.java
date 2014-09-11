package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery3;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery3EmbeddedApi extends Neo4jQuery3<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery3EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query3 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery3Result> execute(final GraphDatabaseService db, final LdbcQuery3 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID,
                operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        Iterator<Node> friends = StepsUtils.excluding(
                StepsUtils.distinct(
                        traversers.friendsAndFriendsOfFriends().traverse(person).nodes()
                ),
                person
        );

        Iterator<Node> friendsNotFromCountryXOrCountryY = Iterators.filter(friends, new Predicate<Node>() {
            @Override
            public boolean apply(Node friend) {
                Relationship personLocatedInCity = friend.getRelationships(Direction.OUTGOING, Domain.Rels.IS_LOCATED_IN).iterator().next();
                Node city = personLocatedInCity.getOtherNode(friend);
                Relationship cityLocatedInCountry = city.getRelationships(Direction.OUTGOING, Domain.Rels.IS_PART_OF).iterator().next();
                Node country = cityLocatedInCountry.getOtherNode(city);
                boolean isNotCountry = false == country.hasLabel(Domain.Place.Type.Country);
                boolean isCountryX = operation.countryXName().equals(country.getProperty(Domain.Place.NAME));
                boolean isCountryY = operation.countryYName().equals(country.getProperty(Domain.Place.NAME));
                return false == (isNotCountry || isCountryX || isCountryY);
            }
        });

        long startDateAsMilli = operation.startDate().getTime();
        int durationHours = operation.durationDays() * 24;
        long endDateAsMilli = Time.fromMilli(startDateAsMilli).plus(Duration.fromHours(durationHours)).asMilli();

        TraversalDescription postsInCountryX = traversers.postsAndCommentsInCountryInDateRange(operation.countryXName(), startDateAsMilli, endDateAsMilli);
        TraversalDescription postsInCountryY = traversers.postsAndCommentsInCountryInDateRange(operation.countryYName(), startDateAsMilli, endDateAsMilli);
        Function<Node, LdbcQuery3Result> nodeToLdbcQuery3ResultFun = new NodeToLdbcQuery3ResultFun(postsInCountryX, postsInCountryY);
        Iterator<LdbcQuery3Result> resultWithZeroCounts = Iterators.transform(friendsNotFromCountryXOrCountryY, nodeToLdbcQuery3ResultFun);

        List<LdbcQuery3Result> result =
                Lists.newArrayList(
                        Iterators.limit(
                                Iterators.filter(
                                        resultWithZeroCounts,
                                        new Predicate<LdbcQuery3Result>() {
                                            @Override
                                            public boolean apply(LdbcQuery3Result input) {
                                                return input.xCount() > 0 && input.yCount() > 0;
                                            }
                                        }),
                                operation.limit()
                        )
                );

        Collections.sort(result, new CountComparator());
        return result.iterator();
    }

    class NodeToLdbcQuery3ResultFun implements Function<Node, LdbcQuery3Result> {
        private final TraversalDescription postsInCountryX;
        private final TraversalDescription postsInCountryY;

        public NodeToLdbcQuery3ResultFun(TraversalDescription postsInCountryX, TraversalDescription postsInCountryY) {
            this.postsInCountryX = postsInCountryX;
            this.postsInCountryY = postsInCountryY;
        }

        @Override
        public LdbcQuery3Result apply(Node friend) {
            int countryXPostCount = Iterables.size(postsInCountryX.traverse(friend));
            int countryYPostCount = Iterables.size(postsInCountryY.traverse(friend));
            long friendId = (long) friend.getProperty(Domain.Person.ID);
            String friendFirstName = (String) friend.getProperty(Domain.Person.FIRST_NAME);
            String friendLastName = (String) friend.getProperty(Domain.Person.LAST_NAME);
            return new LdbcQuery3Result(friendId, friendFirstName, friendLastName, countryXPostCount, countryYPostCount, countryXPostCount + countryYPostCount);
        }
    }

    class CountComparator implements Comparator<LdbcQuery3Result> {
        @Override
        public int compare(LdbcQuery3Result result1, LdbcQuery3Result result2) {
            if (result1.count() > result2.count()) return -1;
            else if (result1.count() < result2.count()) return 1;
            else {
                if (result1.personId() < result2.count()) return -1;
                else if (result1.personId() > result2.count()) return 1;
                else return 0;
            }
        }
    }

}
