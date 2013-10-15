package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.List;

import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.LdbcQuery6;
import com.ldbc.socialnet.workload.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery6;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

public class EmbeddedNeo4jLdbcQuery6Handler extends OperationHandler<LdbcQuery6>
{
    private final static Logger logger = Logger.getLogger( EmbeddedNeo4jLdbcQuery6Handler.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery6 operation ) throws DbException
    {
        ExecutionEngine engine = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).executionEngine();
        GraphDatabaseService db = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).db();
        Neo4jQuery6 query6 = new Neo4jQuery6EmbeddedCypher();
        List<LdbcQuery6Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try
        {
            result = Utils.iteratorToList( query6.execute( db, engine, operation ) );
        }
        catch ( Exception e )
        {
            logger.error( String.format( "Error executing query\n%s", Utils.stackTraceToString( e ) ) );
            resultCode = -1;
        }

        return operation.buildResult( resultCode, result );
    }
}
