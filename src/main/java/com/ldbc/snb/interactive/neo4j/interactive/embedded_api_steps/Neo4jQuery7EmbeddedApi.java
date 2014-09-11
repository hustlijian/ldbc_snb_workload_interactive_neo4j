package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery7;
import org.neo4j.graphdb.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Neo4jQuery7EmbeddedApi extends Neo4jQuery7<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery7EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query7 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery7Result> execute(GraphDatabaseService db, LdbcQuery7 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        //<liker,(message, likeTime)>
        Map<Node, Tuple.Tuple2<Node, Long>> likerPersonsAndTheirLikes = new HashMap<>();
        for (Path path : traversers.personsThatLikedMessageCreatedByPerson().traverse(person)) {
            List<PropertyContainer> pathAsList = Lists.newArrayList(path);
            Node message = (Node) pathAsList.get(2);
            long likeTime = (long) pathAsList.get(3).getProperty(Domain.Likes.CREATION_DATE);
            Node likerPerson = (Node) pathAsList.get(4);
            Tuple.Tuple2<Node, Long> likerPersonsLike = likerPersonsAndTheirLikes.get(likerPerson);
            if (null == likerPersonsLike) {
                likerPersonsLike = Tuple.tuple2(message, likeTime);
            } else {
                long prevLikeTime = likerPersonsLike._2();
                if (likeTime > prevLikeTime) {
                    likerPersonsLike = Tuple.tuple2(message, likeTime);
                } else if (likeTime == prevLikeTime) {
                    long likedMessageId = (long) message.getProperty(Domain.Message.ID);
                    long prevLikedMessageId = (long) likerPersonsLike._1().getProperty(Domain.Message.ID);
                    if (likedMessageId < prevLikedMessageId) likerPersonsLike = Tuple.tuple2(message, likeTime);
                }
            }
            likerPersonsAndTheirLikes.put(likerPerson, likerPersonsLike);
        }

        List<LdbcQuery7Result> results = Lists.newArrayList(
                Iterables.transform(
                        likerPersonsAndTheirLikes.entrySet(), new Function<Map.Entry<Node, Tuple.Tuple2<Node, Long>>, LdbcQuery7Result>() {
                            @Override
                            public LdbcQuery7Result apply(Map.Entry<Node, Tuple.Tuple2<Node, Long>> input) {
                                Node liker = input.getKey();
                                Node message = input.getValue()._1();
                                long personId = (long) liker.getProperty(Domain.Person.ID);
                                String personFirstName = (String) liker.getProperty(Domain.Person.FIRST_NAME);
                                String personLastName = (String) liker.getProperty(Domain.Person.LAST_NAME);
                                long likeCreationDate = input.getValue()._2();
                                long commentOrPostId = (long) message.getProperty(Domain.Message.ID);
                                String commentOrPostContent = (String) message.getProperty(Domain.Message.CONTENT);
                                long commentOrPostCreationDate = (long) message.getProperty(Domain.Message.CREATION_DATE);
                                Long minutesLatency = Time.fromMilli(likeCreationDate).durationGreaterThan(Time.fromMilli(commentOrPostCreationDate)).as(TimeUnit.MINUTES);
                                boolean isNew = false == likerKnowsPerson(liker, person);
                                return new LdbcQuery7Result(personId, personFirstName, personLastName, likeCreationDate, commentOrPostId, commentOrPostContent, minutesLatency.intValue(), isNew);
                            }
                        }
                )
        );

        Collections.sort(results, new DescendingLikeTimeThenAscendingLikerIdComparator());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    private boolean likerKnowsPerson(Node liker, Node person) {
        for (Relationship knowsRelationship : liker.getRelationships(Domain.Rels.KNOWS)) {
            Node otherPerson = knowsRelationship.getOtherNode(liker);
            if (otherPerson.equals(person)) return true;
        }
        return false;
    }

    public static class DescendingLikeTimeThenAscendingLikerIdComparator implements Comparator<LdbcQuery7Result> {
        @Override
        public int compare(LdbcQuery7Result result1, LdbcQuery7Result result2) {
            if (result1.likeCreationDate() > result2.likeCreationDate()) return -1;
            else if (result1.likeCreationDate() < result2.likeCreationDate()) return 1;
            else {
                if (result1.personId() > result2.personId()) return 1;
                else if (result1.personId() < result2.personId()) return -1;
                else return 0;
            }
        }
    }
}
