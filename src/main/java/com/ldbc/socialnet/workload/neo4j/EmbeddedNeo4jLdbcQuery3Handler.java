package com.ldbc.socialnet.workload.neo4j;

import java.util.Map;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.operations.LdbcQuery3;

public class EmbeddedNeo4jLdbcQuery3Handler extends OperationHandler<LdbcQuery3>
{
    @Override
    protected OperationResult executeOperation( LdbcQuery3 operation ) throws DbException
    {
        String query = Queries.LdbcInteractive.Query3.QUERY_TEMPLATE;
        Map<String, Object> params = Queries.LdbcInteractive.Query3.buildParams( operation.getPersonId(),
                operation.getCountryX(), operation.getCountryY(), operation.getStartDate(), operation.getDurationDays() );
        ( (Neo4jConnectionStateEmbedded) getDbConnectionState() ).getExecutionEngine().execute( query, params );
        // TODO find way to do this
        int resultCode = 0;
        // TODO return what query actually returns
        int result = 0;
        return operation.buildResult( resultCode, result );
    }
}
