package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.traversal.steps.execution.StepsUtils;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.util.Function2;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.LdbcQuery5Result;
import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery5;

public class Neo4jQuery5EmbeddedApi implements Neo4jQuery5
{
    private final LdbcTraversers traversers;

    public Neo4jQuery5EmbeddedApi( LdbcTraversers traversers )
    {
        this.traversers = traversers;
    }

    @Override
    public String description()
    {
        return "LDBC Query5 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery5Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery5 operation )
    {
        /*
        What are the groups that your connections (friendship up to second hop) have joined after a certain date? 
        Order them by the number of posts and comments your connections made there.
        
        MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person)
        USING INDEX person:Person(id)
        WHERE person.id={person_id}
        WITH friend
        MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
        WHERE membership.joinDate>{join_date}
        WITH forum, friend
        MATCH (friend)<-[:HAS_CREATOR]-(comment:Comment)
        WHERE (comment)-[:REPLY_OF*0..]->(:Comment)-[:REPLY_OF]->(:Post)<-[:CONTAINER_OF]-(forum)
        RETURN forum.title AS forum, count(comment) AS commentCount
        ORDER BY commentCount DESC
        
        MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person)
        USING INDEX person:Person(id)
        WHERE person.id={person_id}
        WITH friend
        MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
        WHERE membership.joinDate>{join_date}
        WITH forum, friend
        MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
        RETURN forum.title AS forum, count(post) AS postCount
        ORDER BY postCount DESC
         */

        /*
        MATCH (person:Person)-[:KNOWS*1..2]-(friend:Person)
        WHERE person.id={person_id}
         */
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty( Domain.Nodes.Person, Domain.Person.ID,
                operation.personId() ).iterator();
        if ( false == personIterator.hasNext() ) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        Iterator<Node> friendsIterator = StepsUtils.excluding(
                StepsUtils.distinct( traversers.friendsAndFriendsOfFriends().traverse( person ).nodes() ), person );
        Set<Node> friendsSet = ImmutableSet.copyOf( friendsIterator );
        Node[] friendsArray = friendsSet.toArray( new Node[friendsSet.size()] );

        /*
        MATCH (friend)<-[membership:HAS_MEMBER]-(forum:Forum)
        WHERE membership.joinDate>{join_date}
         */
        List<Node> forums = ImmutableList.copyOf( StepsUtils.distinct( traversers.personsMembershipForums(
                operation.joinDate().getTime() ).traverse( friendsArray ).nodes().iterator() ) );

        /*
        MATCH (friend)<-[:HAS_CREATOR]-(comment:Comment)
        WHERE (comment)-[:REPLY_OF*0..]->(:Comment)-[:REPLY_OF]->(:Post)<-[:CONTAINER_OF]-(forum)
         */
        Set<Node> friendsComments = ImmutableSet.copyOf( traversers.personsComments().traverse( friendsArray ).nodes() );
        TraversalDescription commentsOnPostsInForumTraverser = traversers.commentsOnPostsInForum( friendsComments );

        Map<Node, LdbcQuery5Result> forumCommentsMap = new HashMap<Node, LdbcQuery5Result>();
        for ( Node forum : forums )
        {
            String forumTitle = (String) forum.getProperty( Domain.Forum.TITLE );
            int postCount = 0;
            int commentCount = Iterables.size( commentsOnPostsInForumTraverser.traverse( forum ) );
            if ( commentCount > 0 )
                forumCommentsMap.put( forum, new LdbcQuery5Result( forumTitle, postCount, commentCount ) );
        }

        /*
        MATCH (friend)<-[:HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
         */
        TraversalDescription postsInForumByFriendsTraverser = traversers.postsInForumByFriends( friendsSet );
        Map<Node, LdbcQuery5Result> forumPostsMap = new HashMap<Node, LdbcQuery5Result>();
        for ( final Node forum : forums )
        {
            String forumTitle = (String) forum.getProperty( Domain.Forum.TITLE );
            int postCount = Iterables.size( postsInForumByFriendsTraverser.traverse( forum ) );
            int commentCount = 0;
            if ( postCount > 0 )
                forumPostsMap.put( forum, new LdbcQuery5Result( forumTitle, postCount, commentCount ) );
        }

        /*
         * Join
         */
        Function2<LdbcQuery5Result, LdbcQuery5Result, LdbcQuery5Result> joinFun = new Function2<LdbcQuery5Result, LdbcQuery5Result, LdbcQuery5Result>()
        {
            @Override
            public LdbcQuery5Result apply( LdbcQuery5Result from1, LdbcQuery5Result from2 )
            {
                return new LdbcQuery5Result( from1.forumTitle(), from1.postCount() + from2.postCount(),
                        from1.commentCount() + from2.commentCount() );
            }
        };
        Map<Node, LdbcQuery5Result> postsAndCommentsMap = MapUtils.mergeMaps( forumPostsMap, forumCommentsMap, joinFun );

        /*
        ORDER BY commentCount + postCount DESC
         */
        List<LdbcQuery5Result> results = Lists.newArrayList( postsAndCommentsMap.values() );
        Collections.sort( results, new CommentAndPostCountComparator() );

        return results.iterator();
    }

    public static class CommentAndPostCountComparator implements Comparator<LdbcQuery5Result>
    {
        @Override
        public int compare( LdbcQuery5Result result1, LdbcQuery5Result result2 )
        {
            if ( result1.count() == result2.count() ) return 0;
            if ( result1.count() > result2.count() ) return -1;
            return 1;
        }
    }

}
