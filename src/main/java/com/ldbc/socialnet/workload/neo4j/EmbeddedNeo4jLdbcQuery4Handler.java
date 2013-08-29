package com.ldbc.socialnet.workload.neo4j;

import java.util.Map;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.operations.LdbcQuery4;

public class EmbeddedNeo4jLdbcQuery4Handler extends OperationHandler<LdbcQuery4>
{
    @Override
    protected OperationResult executeOperation( LdbcQuery4 operation ) throws DbException
    {
        String query = Queries.LdbcInteractive.Query4.QUERY_TEMPLATE;
        Map<String, Object> params = Queries.LdbcInteractive.Query4.buildParams( operation.getPersonId(),
                operation.getStartDate(), operation.getDurationDays() );
        ( (Neo4jConnectionStateEmbedded) getDbConnectionState() ).getExecutionEngine().execute( query, params );
        // TODO find way to do this
        int resultCode = 0;
        // TODO return what query actually returns
        int result = 0;
        return operation.buildResult( resultCode, result );
    }
}
