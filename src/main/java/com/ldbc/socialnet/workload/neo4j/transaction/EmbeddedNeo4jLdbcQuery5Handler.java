package com.ldbc.socialnet.workload.neo4j.transaction;

import java.util.Map;

import org.apache.log4j.Logger;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;

public class EmbeddedNeo4jLdbcQuery5Handler extends OperationHandler<LdbcQuery5>
{
    private final static Logger logger = Logger.getLogger( EmbeddedNeo4jLdbcQuery5Handler.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery5 operation ) throws DbException
    {
        Map<String, Object> params = Queries.Query5.buildParams( operation.getPersonId(), operation.getJoinDate() );
        String postsQuery = Queries.Query5.QUERY_TEMPLATE_posts;
        String commentsQuery = Queries.Query5.QUERY_TEMPLATE_comments;

        // TODO find way to do this
        int resultCode = 0;
        try
        {
            ( (Neo4jConnectionStateEmbedded) getDbConnectionState() ).getExecutionEngine().execute( postsQuery, params );
            ( (Neo4jConnectionStateEmbedded) getDbConnectionState() ).getExecutionEngine().execute( commentsQuery,
                    params );
        }
        catch ( Exception e )
        {
            logger.error( String.format( "Error encountered executing %s\n%s", getClass().getSimpleName(),
                    e.getMessage() ) );
            resultCode = -1;
        }

        // TODO return what query actually returns
        int result = 0;
        return operation.buildResult( resultCode, result );
    }
}
