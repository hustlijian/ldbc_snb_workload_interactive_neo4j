package com.ldbc.socialnet.workload.neo4j;

import java.util.Map;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.operations.LdbcQuery1;

public class EmbeddedNeo4jLdbcQuery1Handler extends OperationHandler<LdbcQuery1>
{
    @Override
    protected OperationResult executeOperation( LdbcQuery1 operation ) throws DbException
    {
        String query = Queries.LdbcInteractive.Query1.QUERY_TEMPLATE;
        Map<String, Object> params = Queries.LdbcInteractive.Query1.buildParams( operation.getFirstName() );
        ( (Neo4jConnectionStateEmbedded) getDbConnectionState() ).getExecutionEngine().execute( query, params );
        // TODO find way to do this
        int resultCode = 0;
        // TODO return what query actually returns
        int result = 0;
        return operation.buildResult( resultCode, result );
    }
}
