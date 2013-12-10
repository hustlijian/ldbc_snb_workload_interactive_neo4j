package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps;

import java.util.List;

import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery6;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

public class LdbcQuery6HandlerEmbeddedApi extends OperationHandler<LdbcQuery6>
{
    private final static Logger logger = Logger.getLogger( LdbcQuery6HandlerEmbeddedApi.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery6 operation ) throws DbException
    {
        ExecutionEngine engine = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).executionEngine();
        GraphDatabaseService db = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).db();
        LdbcTraversers traversers = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).traversers();
        Neo4jQuery6 query6 = new Neo4jQuery6EmbeddedApi( traversers );
        List<LdbcQuery6Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx())
        {
            result = Utils.iteratorToList( query6.execute( db, engine, operation ) );
            tx.success();
        }
        catch ( Exception e )
        {
            logger.error( String.format( "Error executing query\n%s", Utils.stackTraceToString( e ) ) );
            resultCode = -1;
        }

        return operation.buildResult( resultCode, result );
    }
}