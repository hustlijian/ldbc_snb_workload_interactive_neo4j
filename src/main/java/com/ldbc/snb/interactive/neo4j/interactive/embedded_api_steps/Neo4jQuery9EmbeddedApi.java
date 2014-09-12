package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery9;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery9EmbeddedApi extends Neo4jQuery9<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery9EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query9 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery9Result> execute(GraphDatabaseService db, LdbcQuery9 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Collections.emptyIterator();
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

        List<LdbcQuery9Result> results = Lists.newArrayList(
                Iterables.transform(
                        traversers.commentsAndPostsByPersonCreatedBeforeDate(operation.maxDate().getTime()).traverse(friends),
                        new Function<Path, LdbcQuery9Result>() {
                            @Override
                            public LdbcQuery9Result apply(Path path) {
                                Node friend = path.startNode();
                                Node message = path.endNode();
                                long personId = (long) friend.getProperty(Domain.Person.ID);
                                String personFirstName = (String) friend.getProperty(Domain.Person.FIRST_NAME);
                                String personLastName = (String) friend.getProperty(Domain.Person.LAST_NAME);
                                long messageId = (long) message.getProperty(Domain.Message.ID);
                                String messageContent = (String) message.getProperty(Domain.Message.CONTENT);
                                long messageCreationDate = (long) message.getProperty(Domain.Message.CREATION_DATE);
                                return new LdbcQuery9Result(personId, personFirstName, personLastName, messageId, messageContent, messageCreationDate);
                            }
                        }
                )
        );

        Collections.sort(results, new DescendingMessageCreationDateAscendingMessageId());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    public static class DescendingMessageCreationDateAscendingMessageId implements Comparator<LdbcQuery9Result> {
        @Override
        public int compare(LdbcQuery9Result result1, LdbcQuery9Result result2) {
            if (result1.commentOrPostCreationDate() > result2.commentOrPostCreationDate()) return -1;
            else if (result1.commentOrPostCreationDate() < result2.commentOrPostCreationDate()) return 1;
            else {
                if (result1.commentOrPostId() < result2.commentOrPostId()) return -1;
                else if (result1.commentOrPostId() > result2.commentOrPostId()) return 1;
                else return 0;
            }
        }
    }
}
