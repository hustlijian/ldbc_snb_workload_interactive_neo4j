package com.ldbc.socialnet.workload.neo4j.transaction;

import java.util.Map;

import org.apache.log4j.Logger;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.LdbcQuery4;
import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

public class EmbeddedNeo4jLdbcQuery4Handler extends OperationHandler<LdbcQuery4>
{
    private final static Logger logger = Logger.getLogger( EmbeddedNeo4jLdbcQuery4Handler.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery4 operation ) throws DbException
    {
        String query = Queries.Query4.QUERY_TEMPLATE;
        Map<String, Object> params = Queries.Query4.buildParams( operation.getPersonId(), operation.getStartDate(),
                operation.getDurationDays() );

        // TODO find way to do this
        int resultCode = 0;
        try
        {
            ( (Neo4jConnectionStateEmbedded) getDbConnectionState() ).getExecutionEngine().execute( query, params );
        }
        catch ( Exception e )
        {
            logger.error( String.format( "Error executing query\n%s", Utils.stackTraceToString( e ) ) );
            resultCode = -1;
        }

        // TODO return what query actually returns
        int result = 0;
        return operation.buildResult( resultCode, result );
    }
}
