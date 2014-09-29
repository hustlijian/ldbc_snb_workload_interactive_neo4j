package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.Domain.Message;
import com.ldbc.snb.interactive.neo4j.Domain.Person;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery2;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery2EmbeddedApi extends Neo4jQuery2<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery2EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query2 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery2Result> execute(GraphDatabaseService db, LdbcQuery2 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Collections.emptyIterator();
        final Node person = personIterator.next();

        Iterator<Path> friendsAndPostsPaths = traversers.friendsPostsAndCommentsAtOrBeforeDate(operation.maxDate().getTime()).traverse(person).iterator();

        Iterator<LdbcQuery2Result> friendsPostsResultsIterator = Iterators.transform(friendsAndPostsPaths,
                new Function<Path, LdbcQuery2Result>() {
                    @Override
                    public LdbcQuery2Result apply(Path friendAndPostPath) {
                        List<Node> friendAndPostPathList = ImmutableList.copyOf(friendAndPostPath.nodes());
                        Node friend = friendAndPostPathList.get(1);
                        Node post = friendAndPostPathList.get(2);
                        return new LdbcQuery2Result(
                                (long) friend.getProperty(Person.ID),
                                (String) friend.getProperty(Person.FIRST_NAME),
                                (String) friend.getProperty(Person.LAST_NAME),
                                (long) post.getProperty(Message.ID),
                                (post.hasProperty(Message.CONTENT)) ? (String) post.getProperty(Message.CONTENT) : (String) post.getProperty(Domain.Post.IMAGE_FILE),
                                (long) post.getProperty(Message.CREATION_DATE)
                        );
                    }
                });
        List<LdbcQuery2Result> friendsPostsResults = Lists.newArrayList(friendsPostsResultsIterator);

        Collections.sort(friendsPostsResults, new CreationDateComparator());
        return Iterables.limit(friendsPostsResults, operation.limit()).iterator();
    }

    public static class CreationDateComparator implements Comparator<LdbcQuery2Result> {
        @Override
        public int compare(LdbcQuery2Result result1, LdbcQuery2Result result2) {
            if (result1.postOrCommentCreationDate() > result2.postOrCommentCreationDate()) return -1;
            else if (result1.postOrCommentCreationDate() < result2.postOrCommentCreationDate()) return 1;
            else {
                if (result1.postOrCommentId() < result2.postOrCommentId()) return -1;
                else if (result1.postOrCommentId() > result2.postOrCommentId()) return 1;
                else return 0;
            }
        }
    }

}
