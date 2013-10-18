package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.List;

import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.LdbcQuery5Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery5;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

public class LdbcQuery5HandlerEmbeddedCypher extends OperationHandler<LdbcQuery5>
{
    private final static Logger logger = Logger.getLogger( LdbcQuery5HandlerEmbeddedCypher.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery5 operation ) throws DbException
    {
        ExecutionEngine engine = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).executionEngine();
        GraphDatabaseService db = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).db();
        Neo4jQuery5 query5 = new Neo4jQuery5EmbeddedCypher();
        List<LdbcQuery5Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try
        {
            result = Utils.iteratorToList( query5.execute( db, engine, operation ) );
        }
        catch ( Exception e )
        {
            logger.error( String.format( "Error executing query\n%s", Utils.stackTraceToString( e ) ) );
            resultCode = -1;
        }

        return operation.buildResult( resultCode, result );
    }
}
