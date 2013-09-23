package com.ldbc.socialnet.workload.neo4j.transaction;

import java.util.Map;

import org.apache.log4j.Logger;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;

public class EmbeddedNeo4jLdbcQuery1Handler extends OperationHandler<LdbcQuery1>
{
    private final static Logger logger = Logger.getLogger( EmbeddedNeo4jLdbcQuery1Handler.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery1 operation ) throws DbException
    {
        String query = Queries.Query1.QUERY_TEMPLATE;
        // TODO make limit param later?
        int limit = 10;
        Map<String, Object> params = Queries.Query1.buildParams( operation.getFirstName(), limit );
        // TODO find way to do this
        int resultCode = 0;
        try
        {
            ( (Neo4jConnectionStateEmbedded) getDbConnectionState() ).getExecutionEngine().execute( query, params );
        }
        catch ( Exception e )
        {
            logger.error( String.format( "Error executing query\n%s", e ) );
            resultCode = -1;
        }

        // TODO return what query actually returns
        int result = 0;
        return operation.buildResult( resultCode, result );
    }
}
