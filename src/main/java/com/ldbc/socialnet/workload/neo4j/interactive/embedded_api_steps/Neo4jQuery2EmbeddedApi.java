package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.Domain.Person;
import com.ldbc.socialnet.workload.neo4j.Domain.Post;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery2;
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
        /*
        MATCH (:Person {id:{person_id}})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(post:Post)
        WHERE post.creationDate<={max_date}
         */
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID,
                operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        Iterator<Path> friendsAndPostsPaths = traversers.friendsPostsBeforeDate(operation.maxDateAsMilli()).traverse(
                person).iterator();

        /*
        RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName,
            post.id AS postId, post.content AS postContent, post.creationDate AS postDate
         */
        Iterator<LdbcQuery2Result> friendsPostsResultsIterator = Iterators.transform(friendsAndPostsPaths,
                new Function<Path, LdbcQuery2Result>() {
                    @Override
                    public LdbcQuery2Result apply(Path friendAndPostPath) {
                        List<Node> friendAndPostPathList = ImmutableList.copyOf(friendAndPostPath.nodes());
                        Node friend = friendAndPostPathList.get(1);
                        Node post = friendAndPostPathList.get(2);
                        return new LdbcQuery2Result((long) friend.getProperty(Person.ID),
                                (String) friend.getProperty(Person.FIRST_NAME),
                                (String) friend.getProperty(Person.LAST_NAME), (long) post.getProperty(Post.ID),
                                (String) post.getProperty(Post.CONTENT), (long) post.getProperty(Post.CREATION_DATE));
                    }
                });
        List<LdbcQuery2Result> friendsPostsResults = Lists.newArrayList(friendsPostsResultsIterator);

        /*
        ORDER BY postDate DESC
        LIMIT 20
         */
        Collections.sort(friendsPostsResults, new CreationDateComparator());
        return Iterables.limit(friendsPostsResults, operation.limit()).iterator();
    }

    public static class CreationDateComparator implements Comparator<LdbcQuery2Result> {
        @Override
        public int compare(LdbcQuery2Result result1, LdbcQuery2Result result2) {
            if (result1.postDate() == result2.postDate()) return 0;
            if (result1.postDate() > result2.postDate()) return -1;
            return 1;
        }
    }

}
