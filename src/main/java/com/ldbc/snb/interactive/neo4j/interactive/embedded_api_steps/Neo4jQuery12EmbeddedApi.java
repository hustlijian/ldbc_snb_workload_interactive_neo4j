package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery12;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import java.util.*;

public class Neo4jQuery12EmbeddedApi extends Neo4jQuery12<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery12EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query12 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery12Result> execute(GraphDatabaseService db, LdbcQuery12 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Collections.emptyIterator();
        final Node person = personIterator.next();

        List<Node> friends = new ArrayList<>();
        for (Relationship knows : person.getRelationships(Domain.Rels.KNOWS)) {
            friends.add(knows.getOtherNode(person));
        }

        Iterator<Node> tagClassIterator = db.findNodesByLabelAndProperty(Domain.Nodes.TagClass, Domain.TagClass.NAME, operation.tagClassName()).iterator();
        if (false == tagClassIterator.hasNext()) return Iterators.emptyIterator();
        final Node tagClass = tagClassIterator.next();

        // <person,([tagNames],[comment])>
        Map<Node, Tuple.Tuple2<Set<String>, Set<Node>>> preResults = new HashMap<>();
        for (Node friend : friends) {
            preResults.put(friend, Tuple.tuple2((Set<String>) new HashSet<String>(), (Set<Node>) new HashSet<Node>()));
        }
        for (Path path : traversers.commentsInReplyToPostsTaggedWithTagInGivenTagClassOrDescendentOfThatTagClass(tagClass).traverse(friends.toArray(new Node[friends.size()]))) {
            List<Node> pathNodes = Lists.newArrayList(path.nodes());
            Node friend = pathNodes.get(0);
            Node comment = pathNodes.get(1);
            Node tag = pathNodes.get(3);
            String tagName = (String) tag.getProperty(Domain.Tag.NAME);
            Tuple.Tuple2<Set<String>, Set<Node>> preResult = preResults.get(friend);
            Set<String> tagNames = preResult._1();
            tagNames.add(tagName);
            Set<Node> replies = preResult._2();
            replies.add(comment);
            preResults.put(friend, Tuple.tuple2(tagNames, replies));
        }

        List<LdbcQuery12Result> results = Lists.newArrayList(
                Iterables.transform(
                        preResults.entrySet(),
                        new Function<Map.Entry<Node, Tuple.Tuple2<Set<String>, Set<Node>>>, LdbcQuery12Result>() {
                            @Override
                            public LdbcQuery12Result apply(Map.Entry<Node, Tuple.Tuple2<Set<String>, Set<Node>>> preResult) {
                                Node friend = preResult.getKey();
                                Set<String> tagNames = preResult.getValue()._1();
                                int replyCount = preResult.getValue()._2().size();
                                long personId = (long) friend.getProperty(Domain.Person.ID);
                                String personFirstName = (String) friend.getProperty(Domain.Person.FIRST_NAME);
                                String personLastName = (String) friend.getProperty(Domain.Person.LAST_NAME);
                                return new LdbcQuery12Result(personId, personFirstName, personLastName, tagNames, replyCount);
                            }
                        }
                )
        );

        Collections.sort(results, new DescendingCommentCountAscendingPersonIdentifier());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    public static class DescendingCommentCountAscendingPersonIdentifier implements Comparator<LdbcQuery12Result> {
        @Override
        public int compare(LdbcQuery12Result result1, LdbcQuery12Result result2) {
            if (result1.replyCount() > result2.replyCount()) return -1;
            else if (result1.replyCount() < result2.replyCount()) return 1;
            else {
                if (result1.personId() < result2.personId()) return -1;
                else if (result1.personId() > result2.personId()) return 1;
                else return 0;
            }
        }
    }
}
