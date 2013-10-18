package com.ldbc.socialnet.neo4j.utils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.server.WrappingNeoServerBootstrapper;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.socialnet.neo4j.workload.QueryCorrectnessTest;
import com.ldbc.socialnet.neo4j.workload.TestGraph;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

public class ServerStarter extends Thread
{
    public static void main( String[] args ) throws IOException
    {
        String dbDir = "tempDb";
        GraphDatabaseService db = null;
        ExecutionEngine engine = null;

        FileUtils.deleteRecursively( new File( dbDir ) );
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        engine = new ExecutionEngine( db );

        // TODO uncomment to print CREATE
        System.out.println();
        System.out.println( MapUtils.prettyPrint( TestGraph.Creator.createGraphQueryParams() ) );
        System.out.println( TestGraph.Creator.createGraphQuery() );

        QueryCorrectnessTest.buildGraph( engine, db );
        db.shutdown();

        ServerStarter serverStarter = new ServerStarter( dbDir, Config.NEO4J_RUN_CONFIG );
        serverStarter.start();
    }

    private final String path;
    private final Map<String, String> config;
    private AtomicBoolean stop = new AtomicBoolean( false );
    private AtomicBoolean running = new AtomicBoolean( false );

    public ServerStarter( String path, Map<String, String> config )
    {
        super();
        this.path = path;
        this.config = config;
    };

    public void stopServer()
    {
        stop.set( true );
    }

    public boolean isRunning()
    {
        return running.get();
    }

    @Override
    public void run()
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path ).setConfig( config ).newGraphDatabase();
        WrappingNeoServerBootstrapper server = new WrappingNeoServerBootstrapper( (GraphDatabaseAPI) db );
        server.start();
        running.set( true );
        try
        {
            while ( stop.get() == false )
            {
                Thread.sleep( 50 );
            }
        }
        catch ( InterruptedException e )
        {
        }
        finally
        {
            server.stop();
            db.shutdown();
        }
    }

}
