package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
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

    /*
    Given a start Person, find the Forums which that Person's friends and friends of friends (excluding start Person) became Members of after a given date.
    Return top 20 Forums, and the number of Posts in each Forum that was Created by any of these Persons.
    Sort results descending by the count of Posts, and then ascending by Forum name.
     */
    @Override
    public Iterator<LdbcQuery5Result> execute(GraphDatabaseService db, LdbcQuery5 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        List<Node> friendsList = ImmutableList.copyOf(
                StepsUtils.excluding(
                        StepsUtils.distinct(
                                traversers.friendsAndFriendsOfFriends().traverse(person).nodes()
                        ),
                        person
                )
        );
        Node[] friends = friendsList.toArray(new Node[friendsList.size()]);

        // TODO remove
        System.out.println("minDate = " + operation.minDate().getTime());
        Set<Node> forums = new HashSet<>();
        Set<Node> friendsWhoRecentlyJoinedForumsSet = new HashSet<>();
        for (Path path : traversers.forumsPersonJoinedAfterDate(operation.minDate().getTime()).traverse(friends)) {
            Node friend = path.startNode();
            Node forum = path.endNode();
            // TODO remove
            System.out.println(forum.getProperty(Domain.Forum.TITLE) + "," + friend.getProperty(Domain.Person.FIRST_NAME));
            forums.add(forum);
            friendsWhoRecentlyJoinedForumsSet.add(friend);
        }
        Node[] friendsWhoRecentlyJoinedForums = friendsWhoRecentlyJoinedForumsSet.toArray(new Node[friendsWhoRecentlyJoinedForumsSet.size()]);

        Iterator<Path> postsInForumByFriendsPaths = traversers.postsInForumsByPersons(forums).traverse(friendsWhoRecentlyJoinedForums).iterator();

        Function1<Path, Tuple.Tuple2<String, Node>> extractFun = new Function1<Path, Tuple.Tuple2<String, Node>>() {
            @Override
            public Tuple.Tuple2<String, Node> apply(Path path) {
                List<Node> pathNodes = Lists.newArrayList(path.nodes());
                Node postNode = pathNodes.get(1);
                Node forumNode = pathNodes.get(2);
                String forumTitle = (String) forumNode.getProperty(Domain.Forum.TITLE);
                // TODO remove
                System.out.println("(" + pathNodes.get(0).getProperty(Domain.Person.FIRST_NAME) + ")->(" + postNode.getProperty(Domain.Message.CONTENT) + ")->(" + forumTitle + ")");
                return Tuple.tuple2(forumTitle, postNode);
            }
        };
        Map<String, Collection<Node>> postsGroupedByForumTitle = StepsUtilsTemp.groupBy(postsInForumByFriendsPaths, extractFun, false);
        List<LdbcQuery5Result> results = Lists.newArrayList(
                Iterators.transform(
                        postsGroupedByForumTitle.entrySet().iterator(),
                        new Function<Map.Entry<String, Collection<Node>>, LdbcQuery5Result>() {
                            @Override
                            public LdbcQuery5Result apply(Map.Entry<String, Collection<Node>> forumEntry) {
                                // TODO remove
                                System.out.printf(forumEntry.getKey() + " = ");
                                for (Node n : forumEntry.getValue()) {
                                    System.out.printf(n.getProperty(Domain.Message.CONTENT) + ", ");
                                }
                                System.out.println();

                                return new LdbcQuery5Result(forumEntry.getKey(), forumEntry.getValue().size());
                            }
                        }
                )
        );

//        /*
//        MATCH (friend)<-[:HAS_CREATOR]-(comment:Comment)
//        WHERE (comment)-[:REPLY_OF*0..]->(:Comment)-[:REPLY_OF]->(:Post)<-[:CONTAINER_OF]-(forum)
//         */
//        Set<Node> friendsComments = ImmutableSet.copyOf(traversers.commentsByPerson().traverse(friends).nodes());
//        TraversalDescription commentsOnPostsInForumTraverser = traversers.commentsOnPostsInForum(friendsComments);
//
//        Map<Node, LdbcQuery5Result> forumCommentsMap = new HashMap<>();
//        for (Node forum : forums) {
//            String forumTitle = (String) forum.getProperty(Domain.Forum.TITLE);
//            int postCount = 0;
//            int commentCount = Iterables.size(commentsOnPostsInForumTraverser.traverse(forum));
//            if (commentCount > 0)
//                forumCommentsMap.put(forum, new LdbcQuery5Result(forumTitle, postCount));
//        }
//
//        /*
//        MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
//         */
//        TraversalDescription postsInForumByFriendsTraverser = traversers.postsInForumByFriends(friendsSet);
//        Map<Node, LdbcQuery5Result> forumPostsMap = new HashMap<>();
//        for (final Node forum : forums) {
//            String forumTitle = (String) forum.getProperty(Domain.Forum.TITLE);
//            int postCount = Iterables.size(postsInForumByFriendsTraverser.traverse(forum));
//            int commentCount = 0;
//            if (postCount > 0)
//                forumPostsMap.put(forum, new LdbcQuery5Result(forumTitle, postCount));
//        }
//
//        /*
//         * Join
//         */
//        Function2<LdbcQuery5Result, LdbcQuery5Result, LdbcQuery5Result> joinFun = new Function2<LdbcQuery5Result, LdbcQuery5Result, LdbcQuery5Result>() {
//            @Override
//            public LdbcQuery5Result apply(LdbcQuery5Result from1, LdbcQuery5Result from2) {
//                return new LdbcQuery5Result(from1.forumTitle(), from1.postCount() + from2.postCount());
//            }
//        };
//        Map<Node, LdbcQuery5Result> postsAndCommentsMap = MapUtils.mergeMaps(forumPostsMap, forumCommentsMap, joinFun);

        /*
        ORDER BY commentCount + postCount DESC
         */
//        List<LdbcQuery5Result> results = Lists.newArrayList(postsAndCommentsMap.values());
        Collections.sort(results, new CommentAndPostCountComparator());

        return results.iterator();
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
