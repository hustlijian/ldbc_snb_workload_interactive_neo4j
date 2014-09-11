package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery8;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

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

    /*
    Given a start Person, find (most recent) Comments that are replies to Posts/Comments of the start Person.
    Only consider immediate (1-hop) replies, not the transitive (multi-hop) case.
    Return the top 20 reply Comments, and the Person that created each reply Comment.
    Sort results descending by creation date of reply Comment, and then ascending by identifier of reply Comment.
     */
    @Override
    public Iterator<LdbcQuery8Result> execute(GraphDatabaseService db, LdbcQuery8 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        Iterator<Tuple.Tuple2<Node, Node>> commenterPersonsAndTheirComments = Iterators.transform(
                traversers.commentsThatAreRepliesToPostOrCommentFromStartPerson().traverse(person).iterator(),
                new Function<Path, Tuple.Tuple2<Node, Node>>() {
                    @Override
                    public Tuple.Tuple2<Node, Node> apply(Path path) {
                        List<Node> pathNodes = Lists.newArrayList(path.nodes());
                        Node comment = pathNodes.get(2);
                        Node commenterPerson = pathNodes.get(3);
                        return Tuple.tuple2(commenterPerson, comment);
                    }
                }
        );

        List<LdbcQuery8Result> results = Lists.newArrayList(
                Iterators.transform(
                        commenterPersonsAndTheirComments,
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
