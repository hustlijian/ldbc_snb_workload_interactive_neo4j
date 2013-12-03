package com.ldbc.socialnet.workload.neo4j;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery7;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.LdbcQuery1HandlerEmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.LdbcQuery2HandlerEmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.LdbcQuery3HandlerEmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.LdbcQuery4HandlerEmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.LdbcQuery5HandlerEmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.LdbcQuery6HandlerEmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.LdbcQuery7HandlerEmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

public class Neo4jDbCommandsEmbeddedCypher extends Neo4jDbCommands
{
    final private String path;
    private ExecutionEngine queryEngine;
    private GraphDatabaseService db;
    private DbConnectionState dbConnectionState;

    public Neo4jDbCommandsEmbeddedCypher( String path )
    {
        this.path = path;
    }

    @Override
    public void init()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        queryEngine = new ExecutionEngine( db );
        dbConnectionState = new Neo4jConnectionStateEmbedded( db, queryEngine, null );
        registerShutdownHook( db );
    }

    @Override
    public void cleanUp()
    {
        db.shutdown();
    }

    @Override
    public DbConnectionState getDbConnectionState()
    {
        return dbConnectionState;
    }

    @Override
    public void registerHandlersWithDb( Db db ) throws DbException
    {
        db.registerOperationHandler( LdbcQuery1.class, LdbcQuery1HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery2.class, LdbcQuery2HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery3.class, LdbcQuery3HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery4.class, LdbcQuery4HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery5.class, LdbcQuery5HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery6.class, LdbcQuery6HandlerEmbeddedCypher.class );
        db.registerOperationHandler( LdbcQuery7.class, LdbcQuery7HandlerEmbeddedCypher.class );
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
