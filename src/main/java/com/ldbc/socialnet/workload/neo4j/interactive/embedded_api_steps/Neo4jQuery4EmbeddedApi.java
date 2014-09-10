package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery4;
import org.neo4j.graphdb.*;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.*;
import java.util.Map.Entry;

public class Neo4jQuery4EmbeddedApi extends Neo4jQuery4<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery4EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query4 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery4Result> execute(GraphDatabaseService db, final LdbcQuery4 operation) {
        final long startDateAsMilli = operation.startDate().getTime();
        int durationHours = operation.durationDays() * 24;
        long endDateAsMilli = Time.fromMilli(startDateAsMilli).plus(Duration.fromHours(durationHours)).asMilli();

        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        Node person = personIterator.next();

        Iterator<Path> tagAndPostPaths = traversers.tagsOnPostsCreatedByPersonBetweenDates(startDateAsMilli, endDateAsMilli).traverse(person).iterator();
        Predicate<Path> tagWasTaggedByOldPost = new Predicate<Path>() {
            @Override
            public boolean apply(Path tagAndPostPath) {
                Node tag = tagAndPostPath.endNode();
                for (Relationship messageHasTag : tag.getRelationships(Direction.INCOMING, Domain.Rels.HAS_TAG)) {
                    Node message = messageHasTag.getOtherNode(tag);
                    if (message.hasLabel(Domain.Nodes.Comment)) continue;
                    long postCreationDate = (long) message.getProperty(Domain.Message.CREATION_DATE);
                    if (postCreationDate < startDateAsMilli) return false;
                }
                return true;
            }
        };
        Iterator<Path> recentTagAndPostPaths = Iterators.filter(tagAndPostPaths, tagWasTaggedByOldPost);

        Function<Path, StepsUtils.Pair<String, Node>> extractFun = new Function<Path, StepsUtils.Pair<String, Node>>() {
            @Override
            public StepsUtils.Pair<String, Node> apply(Path path) {
                List<Node> pathNodes = Lists.newArrayList(path.nodes());
                Node tagNode = pathNodes.get(3);
                String tagName = (String) tagNode.getProperty(Domain.Tag.NAME);
                Node postNode = pathNodes.get(2);
                return new StepsUtils.Pair(tagName, postNode);
            }
        };
        Map<String, Collection<Node>> postsGroupedByTagName = StepsUtils.groupBy(recentTagAndPostPaths, extractFun, false);

        Map<String, Integer> tagNamesWithPostCountsMap = new HashMap<>();
        for (String tagName : postsGroupedByTagName.keySet()) {
            tagNamesWithPostCountsMap.put(tagName, postsGroupedByTagName.get(tagName).size());
        }

        List<LdbcQuery4Result> tagCounts = Lists.newArrayList(Iterables.transform(tagNamesWithPostCountsMap.entrySet(),
                new Function<Entry<String, Integer>, LdbcQuery4Result>() {
                    @Override
                    public LdbcQuery4Result apply(Entry<String, Integer> input) {
                        return new LdbcQuery4Result(input.getKey(), input.getValue());
                    }
                }));
        Collections.sort(tagCounts, new PostCountThenTagNameComparator());
        return Iterators.limit(tagCounts.iterator(), operation.limit());
    }

    public static class PostCountThenTagNameComparator implements Comparator<LdbcQuery4Result> {
        @Override
        public int compare(LdbcQuery4Result result1, LdbcQuery4Result result2) {
            if (result1.postCount() > result2.postCount()) return -1;
            else if (result1.postCount() < result2.postCount()) return 1;
            else return result1.tagName().compareTo(result2.tagName());
        }
    }
}
