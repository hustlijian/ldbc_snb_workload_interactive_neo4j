package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps;

import java.util.List;

import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery2;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

public class LdbcQuery2HandlerEmbeddedApi extends OperationHandler<LdbcQuery2>
{
    private final static Logger logger = Logger.getLogger( LdbcQuery2HandlerEmbeddedApi.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery2 operation ) throws DbException
    {
        ExecutionEngine engine = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).executionEngine();
        GraphDatabaseService db = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).db();
        LdbcTraversers traversers = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).traversers();
        Neo4jQuery2 query2 = new Neo4jQuery2EmbeddedApi( traversers );
        List<LdbcQuery2Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx())
        {
            result = Utils.iteratorToList( query2.execute( db, engine, operation ) );
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