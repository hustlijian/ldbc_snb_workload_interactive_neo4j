package com.ldbc.socialnet.workload.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery3;
import com.ldbc.socialnet.workload.LdbcQuery4;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps.LdbcQuery1HandlerEmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps.LdbcQuery3HandlerEmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps.LdbcQuery4HandlerEmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps.LdbcQuery5HandlerEmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps.LdbcTraversersRaw;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps.LdbcTraversersSteps;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

public class Neo4jDbCommandsEmbeddedApi extends Neo4jDbCommands
{
    private final String path;
    private final LdbcTraversersType traversersType;
    private GraphDatabaseService db;
    private DbConnectionState dbConnectionState;
    private LdbcTraversers traversers;

    public enum LdbcTraversersType
    {
        RAW,
        STEPS
    }

    public Neo4jDbCommandsEmbeddedApi( String path, LdbcTraversersType traversersType )
    {
        this.path = path;
        this.traversersType = traversersType;
    }

    @Override
    public void init()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        switch ( traversersType )
        {
        case RAW:
            traversers = new LdbcTraversersRaw( db );
            break;
        case STEPS:
            traversers = new LdbcTraversersSteps( db );
            break;
        default:
            throw new RuntimeException( "Unrecognized LdbcTraversersType: " + traversersType.name() );
        }
        dbConnectionState = new Neo4jConnectionStateEmbedded( db, null, traversers );
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
        db.registerOperationHandler( LdbcQuery1.class, LdbcQuery1HandlerEmbeddedApi.class );
        db.registerOperationHandler( LdbcQuery3.class, LdbcQuery3HandlerEmbeddedApi.class );
        db.registerOperationHandler( LdbcQuery4.class, LdbcQuery4HandlerEmbeddedApi.class );
        db.registerOperationHandler( LdbcQuery5.class, LdbcQuery5HandlerEmbeddedApi.class );
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
