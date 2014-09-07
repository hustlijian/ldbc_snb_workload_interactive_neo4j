package com.ldbc.socialnet.neo4j;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.socialnet.neo4j.workload.TestGraph;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServerBootstrapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class QueryGraphMakerServerStarter {
    public static void main(String[] args) throws IOException {
        TestGraph.QueryGraphMaker queryGraphMaker = new TestGraph.Query14GraphMaker();

        Map dbImportConfig = Utils.loadConfig(TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath());
        String dbDirPath = "/tmp/neodb";
        FileUtils.deleteQuietly(new File(dbDirPath));

        QueryGraphMakerServerStarter queryGraphMakerServerStarter = new QueryGraphMakerServerStarter();
        queryGraphMakerServerStarter.start(queryGraphMaker, dbDirPath, dbImportConfig);
    }

    void start(TestGraph.QueryGraphMaker queryGraphMaker, String path, Map<String, String> config) throws IOException {
        // TODO uncomment to print CREATE
        System.out.println();
        System.out.println(MapUtils.prettyPrint(queryGraphMaker.params()));
        System.out.println(queryGraphMaker.queryString());

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path).setConfig(config).newGraphDatabase();
        ExecutionEngine engine = new ExecutionEngine(db);

        TestGraph.createDbFromCypherQuery(engine, db, queryGraphMaker.queryString(), queryGraphMaker.params());
        db.shutdown();

        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path).setConfig(config).newGraphDatabase();

        registerShutdownHook(db);

        WrappingNeoServerBootstrapper server = new WrappingNeoServerBootstrapper((GraphDatabaseAPI) db);

        // CommunityServerBuilder serverBuilder =
        // CommunityServerBuilder.server();
        // for ( Entry<String, String> property : config.entrySet() )
        // {
        // serverBuilder.withProperty( property.getKey(), property.getValue() );
        // }
        // CommunityNeoServer server = serverBuilder.build();

        server.start();

        try {
            while (true) {
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            server.stop();
        }
    }

    private void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

}
