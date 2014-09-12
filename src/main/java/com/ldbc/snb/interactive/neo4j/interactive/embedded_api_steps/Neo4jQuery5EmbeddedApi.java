package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery5;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.*;

public class Neo4jQuery5EmbeddedApi extends Neo4jQuery5<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery5EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query5 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery5Result> execute(GraphDatabaseService db, final LdbcQuery5 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Collections.emptyIterator();
        final Node person = personIterator.next();

        Set<Node> friends = Sets.newHashSet(
                StepsUtils.excluding(
                        traversers.friendsAndFriendsOfFriends().traverse(person).nodes(),
                        person
                )
        );

        final long minDateAsMilli = operation.minDate().getTime();

        // path -> (Forum, memberPerson)
        Function<Path, StepsUtils.Pair<Node, Node>> extractFun = new Function<Path, StepsUtils.Pair<Node, Node>>() {
            @Override
            public StepsUtils.Pair<Node, Node> apply(Path path) {
                Node memberPerson = path.startNode();
                Node forum = path.endNode();
                return new StepsUtils.Pair(forum, memberPerson);
            }
        };
        // (forum,[memberPerson])
        Map<Node, Collection<Node>> friendsByForums = StepsUtils.groupBy(
                traversers.forumsPersonJoinedAfterDate(minDateAsMilli).traverse(friends.toArray(new Node[friends.size()])).iterator(),
                extractFun,
                false
        );

        //(Forum.title, count(Post), Forum.id)
        List<Tuple.Tuple3<String, Integer, Long>> preResults = Lists.newArrayList(
                Iterables.transform(
                        friendsByForums.entrySet(),
                        new Function<Map.Entry<Node, Collection<Node>>, Tuple.Tuple3<String, Integer, Long>>() {
                            @Override
                            public Tuple.Tuple3<String, Integer, Long> apply(Map.Entry<Node, Collection<Node>> forumAndFriendWithMembership) {
                                Node forum = forumAndFriendWithMembership.getKey();
                                long forumId = (long) forum.getProperty(Domain.Forum.ID);
                                Node[] friendsWithMembership = forumAndFriendWithMembership.getValue().toArray(new Node[forumAndFriendWithMembership.getValue().size()]);
                                String forumTitle = (String) forum.getProperty(Domain.Forum.TITLE);
                                int postCount = Iterables.size(traversers.postsInForumByPersons(forum).traverse(friendsWithMembership));
                                return Tuple.tuple3(forumTitle, postCount, forumId);
                            }
                        }
                )
        );

        Collections.sort(preResults, new DescendingPostCountAscendingForumIdComparator());

        //(Forum.title, count(Post), Forum.id) -> LdbcQuery5Result
        Iterator<LdbcQuery5Result> results = Iterators.transform(
                preResults.iterator(),
                new Function<Tuple.Tuple3<String, Integer, Long>, LdbcQuery5Result>() {
                    @Override
                    public LdbcQuery5Result apply(Tuple.Tuple3<String, Integer, Long> preResult) {
                        return new LdbcQuery5Result(preResult._1(), preResult._2());
                    }
                }
        );

        return Iterators.limit(results, operation.limit());
    }

    //(Forum.title, count(Post), Forum.id)
    public static class DescendingPostCountAscendingForumIdComparator implements Comparator<Tuple.Tuple3<String, Integer, Long>> {
        @Override
        public int compare(Tuple.Tuple3<String, Integer, Long> preResult1, Tuple.Tuple3<String, Integer, Long> preResult2) {
            // descending post count
            if (preResult1._2() < preResult2._2()) return 1;
            else if (preResult1._2() > preResult2._2()) return -1;
            else {
                // ascending forum id
                if (preResult1._3() > preResult2._3()) return 1;
                else if (preResult1._3() < preResult2._3()) return -1;
                else return 0;
            }
        }
    }

}
