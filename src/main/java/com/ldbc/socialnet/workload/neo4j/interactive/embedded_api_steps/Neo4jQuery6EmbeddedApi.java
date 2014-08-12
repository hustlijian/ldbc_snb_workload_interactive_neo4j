package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
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

    @Override
    public Iterator<LdbcQuery6Result> execute(GraphDatabaseService db, LdbcQuery6 operation) {
        /*
        QUERY 6

        STORY 
        
            People who discuss X also discuss.
            Find 10 most popular Tags of people that are connected to you via friendship path and talk about topic/Tag 'X'.
        
        DESCRIPTION

            Among POSTS by FRIENDS and FRIENDS OF FRIENDS, find the TAGS most commonly occurring together with a given TAG.

        PARAMETERS
        
            Person
            Source.tag
                        
        RETURN
        
            Tag.name
            count
         */

        /*
        MATCH (:Person {id:{person_id}})-[:KNOWS*1..2]-(:Person)<-[:HAS_CREATOR]-(post:Post),
            (post)-[:HAS_TAG]->(:Tag {name:{tag_name}})
        WITH DISTINCT post
        MATCH (post)-[:HAS_TAG]->(tag:Tag)
        WHERE NOT(tag.name={tag_name})
        RETURN tag.name AS tagName, count(tag) AS tagCount
        ORDER BY tagCount DESC
        LIMIT 10
         */

        /*
        MATCH (:Person {id:{person_id}})-[:KNOWS*1..2]-(:Person)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag {name:{tag_name}})
        WITH DISTINCT post
         */
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID,
                operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        // TODO uncomment
//        List<Node> friends = ImmutableList.copyOf(StepsUtils.excluding(
//                StepsUtils.distinct(traversers.friendsAndFriendsOfFriends().traverse(person).nodes()), person));
        List<Node> friends = null;
        Node[] friendsArray = friends.toArray(new Node[friends.size()]);

        // TODO uncomment
//        List<Node> friendsPosts = ImmutableList.copyOf(StepsUtils.projectNodesFromPath(
//                traversers.personsPostsWithGivenTag(operation.tagName()).traverse(friendsArray), 1));
        List<Node> friendsPosts = null;

        /*
        MATCH (post)-[:HAS_TAG]->(tag:Tag)
        WHERE NOT(tag.name={tag_name})
         */
        Node[] friendsPostsArray = friendsPosts.toArray(new Node[friendsPosts.size()]);
        Iterator<Node> tagNodes = traversers.tagsOnPosts(operation.tagName()).traverse(friendsPostsArray).nodes().iterator();

        /*
        RETURN tag.name AS tagName, count(tag) AS tagCount
         */
        // TODO uncomment
//        Map<String, Integer> tagNameCounts = StepsUtils.count(Iterators.transform(tagNodes,
//                new Function<Node, String>() {
//                    @Override
//                    public String apply(Node tagNode) {
//                        return (String) tagNode.getProperty(Tag.NAME);
//                    }
//                }));
        Map<String, Integer> tagNameCounts = null;

        List<LdbcQuery6Result> result = Lists.newArrayList(Iterables.transform(tagNameCounts.entrySet(),
                new Function<Entry<String, Integer>, LdbcQuery6Result>() {
                    @Override
                    public LdbcQuery6Result apply(Entry<String, Integer> tagCount) {
                        return new LdbcQuery6Result(tagCount.getKey(), tagCount.getValue());
                    }
                }));

        /*
        ORDER BY tagCount DESC
        LIMIT 10
         */
        Collections.sort(result, new TagCountComparator());
        return Iterators.limit(result.iterator(), operation.limit());
    }

    public static class TagCountComparator implements Comparator<LdbcQuery6Result> {
        @Override
        public int compare(LdbcQuery6Result result1, LdbcQuery6Result result2) {
            if (result1.tagCount() == result2.tagCount()) return 0;
            if (result1.tagCount() > result2.tagCount()) return -1;
            return 1;
        }
    }
}
