package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.util.Function1;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery5;
import com.ldbc.socialnet.workload.neo4j.utils.StepsUtilsTemp;
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
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        Set<Node> friends = Sets.newHashSet(
                StepsUtils.excluding(
                        traversers.friendsAndFriendsOfFriends().traverse(person).nodes(),
                        person
                )
        );

        final long minDateAsMilli = operation.minDate().getTime();

        // path -> (forum,memberPerson)
        Function1<Path, Tuple.Tuple2<Node, Node>> extractFun = new Function1<Path, Tuple.Tuple2<Node, Node>>() {
            @Override
            public Tuple.Tuple2<Node, Node> apply(Path path) {
                Node memberPerson = path.startNode();
                Node forum = path.endNode();
                return Tuple.tuple2(forum, memberPerson);
            }
        };
        // (forum,[memberPerson])
        Map<Node, Collection<Node>> friendsByForums = StepsUtilsTemp.groupBy(
                traversers.forumsPersonJoinedAfterDate(minDateAsMilli).traverse(friends.toArray(new Node[friends.size()])).iterator(),
                extractFun,
                false
        );

        List<LdbcQuery5Result> results = Lists.newArrayList(
                Iterables.transform(
                        friendsByForums.entrySet(),
                        new Function<Map.Entry<Node, Collection<Node>>, LdbcQuery5Result>() {
                            @Override
                            public LdbcQuery5Result apply(Map.Entry<Node, Collection<Node>> forumAndFriendWithMembership) {
                                Node forum = forumAndFriendWithMembership.getKey();
                                Node[] friendsWithMembership = forumAndFriendWithMembership.getValue().toArray(new Node[forumAndFriendWithMembership.getValue().size()]);
                                String forumTitle = (String) forum.getProperty(Domain.Forum.TITLE);
                                int postCount = Iterables.size(traversers.postsInForumByPersons(forum).traverse(friendsWithMembership));
                                return new LdbcQuery5Result(forumTitle, postCount);
                            }
                        }
                )
        );

        Collections.sort(results, new CommentAndPostCountComparator());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    public static class CommentAndPostCountComparator implements Comparator<LdbcQuery5Result> {
        @Override
        public int compare(LdbcQuery5Result result1, LdbcQuery5Result result2) {
            if (result1.postCount() < result2.postCount()) return 1;
            else if (result1.postCount() > result2.postCount()) return -1;
            else return result1.forumTitle().compareTo(result2.forumTitle());
        }
    }

}
