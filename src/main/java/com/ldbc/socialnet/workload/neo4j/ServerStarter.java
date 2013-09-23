//package com.ldbc.socialnet.workload.neo4j;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Properties;
//
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.factory.GraphDatabaseFactory;
//import org.neo4j.kernel.GraphDatabaseAPI;
//import org.neo4j.kernel.impl.util.FileUtils;
//import org.neo4j.server.CommunityNeoServer;
//import org.neo4j.server.WrappingNeoServerBootstrapper;
//
//import com.ldbc.socialnet.workload.neo4j.utils.Config;
//
//public class ServerStarter
//{
//    public static void main( String[] args ) throws IOException
//    {
//        ServerStarter serverStarter = new ServerStarter();
//        serverStarter.start( Config.DB_DIR, Config.NEO4J_RUN_CONFIG );
//    }
//
//    void start( String path, Map<String, String> config ) throws IOException
//    {
//        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path ).setConfig( config ).newGraphDatabase();
//        WrappingNeoServerBootstrapper server = new WrappingNeoServerBootstrapper( (GraphDatabaseAPI) db );
//
//        // CommunityServerBuilder serverBuilder =
//        // CommunityServerBuilder.server();
//        // for ( Entry<String, String> property : config.entrySet() )
//        // {
//        // serverBuilder.withProperty( property.getKey(), property.getValue() );
//        // }
//        // CommunityNeoServer server = serverBuilder.build();
//
//        server.start();
//
//        try
//        {
//            while ( true )
//            {
//                Thread.sleep( 50 );
//            }
//        }
//        catch ( InterruptedException e )
//        {
//            server.stop();
//        }
//    }
//
//    private void registerShutdownHook( final GraphDatabaseService graphDb )
//    {
//        Runtime.getRuntime().addShutdownHook( new Thread()
//        {
//            @Override
//            public void run()
//            {
//                graphDb.shutdown();
//            }
//        } );
//    }
//
// }
