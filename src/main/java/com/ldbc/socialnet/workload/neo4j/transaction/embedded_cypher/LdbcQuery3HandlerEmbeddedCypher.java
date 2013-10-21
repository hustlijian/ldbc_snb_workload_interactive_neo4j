package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.List;

import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.socialnet.workload.LdbcQuery3;
import com.ldbc.socialnet.workload.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery3;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;

public class LdbcQuery3HandlerEmbeddedCypher extends OperationHandler<LdbcQuery3>
{
    private final static Logger logger = Logger.getLogger( LdbcQuery3HandlerEmbeddedCypher.class );

    @Override
    protected OperationResult executeOperation( LdbcQuery3 operation ) throws DbException
    {
        ExecutionEngine engine = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).executionEngine();
        GraphDatabaseService db = ( (Neo4jConnectionStateEmbedded) dbConnectionState() ).db();
        Neo4jQuery3 query3 = new Neo4jQuery3EmbeddedCypher();
        List<LdbcQuery3Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx())
        {
            result = Utils.iteratorToList( query3.execute( db, engine, operation ) );
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
