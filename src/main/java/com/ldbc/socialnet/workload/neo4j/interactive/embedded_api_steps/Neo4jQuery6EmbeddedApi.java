package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery6;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.*;
import java.util.Map.Entry;

public class Neo4jQuery6EmbeddedApi extends Neo4jQuery6<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery6EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query6 Java API Implementation";
    }

    /*
    Given a start Person and some Tag, find the other Tags that occur together with this Tag on Posts that were created by start Person's friends and friends of friends (excluding start Person).
    Return top 10 Tags, and the count of Posts that were created by these Persons, which contain this Tag.
    Sort results descending by count, and then ascending by Tag name.
     */
    @Override
    public Iterator<LdbcQuery6Result> execute(GraphDatabaseService db, LdbcQuery6 operation) {
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

        List<Node> friendsPostsWithGivenTagList = ImmutableList.copyOf(
                StepsUtils.projectNodesFromPath(
                        traversers.personsPostsWithGivenTag(operation.tagName()).traverse(friends),
                        1
                )
        );
        Node[] friendsPostsWithGivenTag = friendsPostsWithGivenTagList.toArray(new Node[friendsPostsWithGivenTagList.size()]);

        Iterator<Node> otherTagsOnFriendsPostsWithGivenTag = traversers.tagsOnPostsExcludingGivenTag(operation.tagName()).traverse(friendsPostsWithGivenTag).nodes().iterator();

        Map<String, Integer> postCountsPerTagName = StepsUtils.count(
                Iterators.transform(
                        otherTagsOnFriendsPostsWithGivenTag,
                        new Function<Node, String>() {
                            @Override
                            public String apply(Node tagNode) {
                                return (String) tagNode.getProperty(Domain.Tag.NAME);
                            }
                        }
                )
        );

        List<LdbcQuery6Result> results = Lists.newArrayList(
                Iterables.transform(
                        postCountsPerTagName.entrySet(),
                        new Function<Entry<String, Integer>, LdbcQuery6Result>() {
                            @Override
                            public LdbcQuery6Result apply(Entry<String, Integer> postCountForTagName) {
                                String tagName = postCountForTagName.getKey();
                                int postCount = postCountForTagName.getValue();
                                return new LdbcQuery6Result(tagName, postCount);
                            }
                        }
                )
        );

        Collections.sort(results, new DescendingPostCountThenAscendingTagNameComparator());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    public static class DescendingPostCountThenAscendingTagNameComparator implements Comparator<LdbcQuery6Result> {
        @Override
        public int compare(LdbcQuery6Result result1, LdbcQuery6Result result2) {
            if (result1.postCount() > result2.postCount()) return -1;
            else if (result1.postCount() < result2.postCount()) return 1;
            else return result1.tagName().compareTo(result2.tagName());
        }
    }
}
