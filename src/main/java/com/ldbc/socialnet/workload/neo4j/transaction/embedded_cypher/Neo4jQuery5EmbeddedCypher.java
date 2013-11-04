package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.ldbc.driver.util.Function2;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.LdbcQuery5Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery5;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

public class Neo4jQuery5EmbeddedCypher implements Neo4jQuery5
{
    @Override
    public String description()
    {
        return queryComments() + "\n" + queryPosts();
    }

    @Override
    public Iterator<LdbcQuery5Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery5 operation )
    {
        Map<String, Object> cypherParams = buildParams( operation.personId(), operation.joinDate() );
        Map<String, LdbcQuery5Result> postsMap = buildPostsMap( engine.execute( queryPosts(), cypherParams ).iterator() );
        Map<String, LdbcQuery5Result> commentsMap = buildCommentsMap( engine.execute( queryComments(), cypherParams ).iterator() );

        Function2<LdbcQuery5Result, LdbcQuery5Result, LdbcQuery5Result> joinFun = new Function2<LdbcQuery5Result, LdbcQuery5Result, LdbcQuery5Result>()
        {
            @Override
            public LdbcQuery5Result apply( LdbcQuery5Result from1, LdbcQuery5Result from2 )
            {
                return new LdbcQuery5Result( from1.forumTitle(), from1.postCount() + from2.postCount(),
                        from1.commentCount() + from2.commentCount() );
            }
        };
        Map<String, LdbcQuery5Result> postsAndCommentsMap = MapUtils.mergeMaps( postsMap, commentsMap, joinFun );

        List<LdbcQuery5Result> postsAndComments = Utils.iteratorToList( postsAndCommentsMap.values().iterator() );

        Collections.sort( postsAndComments, new LdbcQuery5ResultComparator() );
        return postsAndComments.iterator();
    }

    private Map<String, Object> buildParams( long personId, Date date )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", personId );
        queryParams.put( "join_date", date.getTime() );
        return queryParams;
    }

    private String queryComments()
    {
        return "MATCH (person:" + Domain.Node.Person + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(friend:"
               + Domain.Node.Person + ")\n"

               + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.ID + ")\n"

               + "WHERE person." + Domain.Person.ID + "={person_id}\n"

               + "WITH friend\n"

               + "MATCH (friend)<-[membership:" + Domain.Rel.HAS_MEMBER + "]-(forum:" + Domain.Node.Forum + ")\n"

               + "WHERE membership." + Domain.HasMember.JOIN_DATE + ">{join_date}\n"

               + "WITH forum, friend\n"

               + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(comment:" + Domain.Node.Comment + ")\n"

               + "WHERE (comment)-[:" + Domain.Rel.REPLY_OF + "*0..]->(:" + Domain.Node.Comment + ")-[:"
               + Domain.Rel.REPLY_OF + "]->(:" + Domain.Node.Post + ")<-[:" + Domain.Rel.CONTAINER_OF + "]-(forum)\n"

               + "RETURN forum.title AS forum, count(comment) AS commentCount\n"

               + "ORDER BY commentCount DESC";
    }

    private Map<String, LdbcQuery5Result> buildCommentsMap( Iterator<Map<String, Object>> queryCommentsResult )
    {
        Map<String, LdbcQuery5Result> commentsMap = new HashMap<String, LdbcQuery5Result>();
        while ( queryCommentsResult.hasNext() )
        {
            Map<String, Object> row = queryCommentsResult.next();
            String forum = (String) row.get( "forum" );
            commentsMap.put( forum, new LdbcQuery5Result( forum, 0, (long) row.get( "commentCount" ) ) );
        }
        return commentsMap;
    }

    private String queryPosts()
    {
        return "MATCH (person:" + Domain.Node.Person + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(friend:"
               + Domain.Node.Person + ")\n"

               + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.ID + ")\n"

               + "WHERE person." + Domain.Person.ID + "={person_id}\n"

               + "WITH friend\n"

               + "MATCH (friend)<-[membership:" + Domain.Rel.HAS_MEMBER + "]-(forum:" + Domain.Node.Forum + ")\n"

               + "WHERE membership." + Domain.HasMember.JOIN_DATE + ">{join_date}\n"

               + "WITH forum, friend\n"

               + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.Post + ")<-[:"
               + Domain.Rel.CONTAINER_OF + "]-(forum)\n"

               + "RETURN forum.title AS forum, count(post) AS postCount\n"

               + "ORDER BY postCount DESC";

    }

    private Map<String, LdbcQuery5Result> buildPostsMap( Iterator<Map<String, Object>> queryPostsResult )
    {
        Map<String, LdbcQuery5Result> postsMap = new HashMap<String, LdbcQuery5Result>();
        while ( queryPostsResult.hasNext() )
        {
            Map<String, Object> row = queryPostsResult.next();
            String forum = (String) row.get( "forum" );
            postsMap.put( forum, new LdbcQuery5Result( forum, (long) row.get( "postCount" ), 0 ) );

        }
        return postsMap;
    }

    public static class MergeCommentsAndPostsIterator implements Iterator<LdbcQuery5Result>
    {
        private final Iterator<Map<String, Object>> inner;
        private final Map<String, Long> postsMap;

        public MergeCommentsAndPostsIterator( Map<String, Long> postsMap, Iterator<Map<String, Object>> inner )
        {
            this.postsMap = postsMap;
            this.inner = inner;
        }

        @Override
        public boolean hasNext()
        {
            return inner.hasNext();
        }

        @Override
        public LdbcQuery5Result next()
        {
            Map<String, Object> commentsRow = inner.next();
            String forum = (String) commentsRow.get( "forum" );
            long commentCount = (long) commentsRow.get( "commentCount" );
            long postCount = postsMap.get( forum );
            return new LdbcQuery5Result( forum, postCount, commentCount );
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class LdbcQuery5ResultComparator implements Comparator<LdbcQuery5Result>
    {
        @Override
        public int compare( LdbcQuery5Result o1, LdbcQuery5Result o2 )
        {
            if ( o1.count() == o2.count() ) return 0;
            if ( o1.count() > o2.count() ) return -1;
            return 1;
        }
    }
}
