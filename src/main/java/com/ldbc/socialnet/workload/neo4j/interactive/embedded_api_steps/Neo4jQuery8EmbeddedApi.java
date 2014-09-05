package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery8;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery8EmbeddedApi extends Neo4jQuery8<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery8EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query8 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery8Result> execute(GraphDatabaseService db, LdbcQuery8 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        List<Node> personsCommentsAndPosts = Lists.newArrayList(
                traversers.commentsAndPostsByPerson().traverse(person).nodes()
        );

        //(commenterPerson,comment)
        Iterator<Tuple.Tuple2<Node, Node>> commenterPersonsAndTheirComments = Iterators.transform(
                traversers.commentsRepliedToPostOrCommentExcludingThoseByGivenPerson().traverse(personsCommentsAndPosts.toArray(new Node[personsCommentsAndPosts.size()])).iterator(),
                new Function<Path, Tuple.Tuple2<Node, Node>>() {
                    @Override
                    public Tuple.Tuple2<Node, Node> apply(Path path) {
                        Node comment = path.endNode();
                        Node commenterPerson = comment.getRelationships(Domain.Rels.HAS_CREATOR, Direction.OUTGOING).iterator().next().getOtherNode(comment);
                        return Tuple.tuple2(commenterPerson, comment);
                    }
                }
        );

        Iterator<Tuple.Tuple2<Node, Node>> commenterPersonsAndTheirCommentsExcludingStartPerson = StepsUtils.distinct(
                Iterators.filter(
                        commenterPersonsAndTheirComments,
                        new Predicate<Tuple.Tuple2<Node, Node>>() {
                            @Override
                            public boolean apply(Tuple.Tuple2<Node, Node> commenterPersonAndComment) {
                                Node commenterPerson = commenterPersonAndComment._1();
                                return false == commenterPerson.equals(person);
                            }
                        }
                )
        );

        List<LdbcQuery8Result> results = Lists.newArrayList(
                Iterators.transform(
                        commenterPersonsAndTheirCommentsExcludingStartPerson,
                        new Function<Tuple.Tuple2<Node, Node>, LdbcQuery8Result>() {
                            @Override
                            public LdbcQuery8Result apply(Tuple.Tuple2<Node, Node> commenterPersonAndComment) {
                                Node commenterPerson = commenterPersonAndComment._1();
                                Node comment = commenterPersonAndComment._2();
                                long personId = (long) commenterPerson.getProperty(Domain.Person.ID);
                                String personFirstName = (String) commenterPerson.getProperty(Domain.Person.FIRST_NAME);
                                String personLastName = (String) commenterPerson.getProperty(Domain.Person.LAST_NAME);
                                long commentCreationDate = (long) comment.getProperty(Domain.Message.CREATION_DATE);
                                long commentId = (long) comment.getProperty(Domain.Message.ID);
                                String commentContent = (String) comment.getProperty(Domain.Message.CONTENT);
                                return new LdbcQuery8Result(personId, personFirstName, personLastName, commentCreationDate, commentId, commentContent);
                            }
                        }
                )
        );
        Collections.sort(results, new DescendingCommentCreationDateAscendingCommentId());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    public static class DescendingCommentCreationDateAscendingCommentId implements Comparator<LdbcQuery8Result> {
        @Override
        public int compare(LdbcQuery8Result result1, LdbcQuery8Result result2) {
            if (result1.commentCreationDate() > result2.commentCreationDate()) return -1;
            else if (result1.commentCreationDate() < result2.commentCreationDate()) return 1;
            else {
                if (result1.commentId() < result2.commentId()) return -1;
                else if (result1.commentId() > result2.commentId()) return 1;
                else return 0;
            }
        }
    }
}
