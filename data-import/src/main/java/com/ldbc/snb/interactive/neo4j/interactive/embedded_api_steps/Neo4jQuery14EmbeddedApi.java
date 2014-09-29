package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery14;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery14EmbeddedApi extends Neo4jQuery14<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery14EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query14 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(GraphDatabaseService db, LdbcQuery14 operation) {
        Iterator<Node> person1Iterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.person1Id()).iterator();
        if (false == person1Iterator.hasNext()) return Collections.emptyIterator();
        Node person1 = person1Iterator.next();

        Iterator<Node> person2Iterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.person2Id()).iterator();
        if (false == person2Iterator.hasNext()) return Collections.emptyIterator();
        Node person2 = person2Iterator.next();

        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(PathExpanders.forTypeAndDirection(Domain.Rels.KNOWS, Direction.BOTH), Integer.MAX_VALUE);

        List<LdbcQuery14Result> results = Lists.newArrayList(
                Iterables.transform(
                        finder.findAllPaths(person1, person2),
                        new Function<Path, LdbcQuery14Result>() {
                            @Override
                            public LdbcQuery14Result apply(Path shortestPath) {
                                Iterable<Node> personsInPath = shortestPath.nodes();
                                double pathWeight = calculatePathWeight(personsInPath);
                                List<Long> personIdsInPath = Lists.newArrayList(
                                        Iterables.transform(
                                                personsInPath,
                                                new Function<Node, Long>() {
                                                    @Override
                                                    public Long apply(Node person) {
                                                        return (long) person.getProperty(Domain.Person.ID);
                                                    }
                                                }
                                        )
                                );
                                return new LdbcQuery14Result(personIdsInPath, pathWeight);
                            }
                        }
                )
        );

        Collections.sort(results, new DescendingPathWeight());
        return results.iterator();
    }

    double calculatePathWeight(Iterable<Node> personsInPath) {
        double weight = 0;
        Iterator<Node> personsInPathIterator = personsInPath.iterator();
        Node prevPerson = null;
        Node currPerson = personsInPathIterator.next();
        while (personsInPathIterator.hasNext()) {
            prevPerson = currPerson;
            currPerson = personsInPathIterator.next();
            weight += Iterables.size(traversers.commentsMadeInReplyToPostsOfOtherPerson(currPerson).traverse(prevPerson)) * 1.0;
            weight += Iterables.size(traversers.commentsMadeInReplyToPostsOfOtherPerson(prevPerson).traverse(currPerson)) * 1.0;
            weight += Iterables.size(traversers.commentsMadeByPersonInReplyToCommentsOfOtherPerson(currPerson).traverse(prevPerson)) * 0.5;
        }
        return weight;
    }

    public static class DescendingPathWeight implements Comparator<LdbcQuery14Result> {
        @Override
        public int compare(LdbcQuery14Result result1, LdbcQuery14Result result2) {
            if (result1.pathWeight() > result2.pathWeight()) return -1;
            else if (result1.pathWeight() < result2.pathWeight()) return 1;
            else return 0;
        }
    }
}
