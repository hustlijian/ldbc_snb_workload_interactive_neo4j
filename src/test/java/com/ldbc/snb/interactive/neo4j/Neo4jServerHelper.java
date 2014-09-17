package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.snb.interactive.neo4j.interactive.TestGraph;
import org.apache.commons.io.FileUtils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.WrappingNeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;

import java.io.File;
import java.io.IOException;

public class Neo4jServerHelper {
    private static int port = 7475;

    public static synchronized int nextFreePort() {
        return port++;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 7474;
        String path = "/tmp/neodb";
        FileUtils.deleteDirectory(new File(path));
        TestGraph.QueryGraphMaker queryGraphMaker = new TestGraph.Query2GraphMaker();
        WrappingNeoServer wrappingNeoServer = Neo4jServerHelper.fromQueryGraphMaker(queryGraphMaker, path, port);
        wrappingNeoServer.start();
    }

    public static WrappingNeoServer fromQueryGraphMaker(TestGraph.QueryGraphMaker queryGraphMaker, String path, int port) throws IOException {
        FileUtils.deleteDirectory(new File(path));

        System.out.println();
        System.out.println(MapUtils.prettyPrint(queryGraphMaker.params()));
        System.out.println(queryGraphMaker.queryString());

        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(path)
                .loadPropertiesFromFile(TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath())
                .newGraphDatabase();
        ExecutionEngine engine = new ExecutionEngine(db);
        TestGraph.createDbFromCypherQuery(engine, db, queryGraphMaker.queryString(), queryGraphMaker.params());
        db.shutdown();

        return fromPath(path, port);
    }

    public static WrappingNeoServer fromPath(String path, int port) throws IOException {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(path)
                .loadPropertiesFromFile(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath())
                .newGraphDatabase();
        return fromDb(db, port);
    }

    public static WrappingNeoServer fromDb(GraphDatabaseService db, int port) throws IOException {
        Configurator configurator = new ServerConfigurator((GraphDatabaseAPI) db);
        configurator.configuration().addProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, port);
//        return new WrappingNeoServer((GraphDatabaseAPI) db, configurator) {
//            @Override
//            protected Iterable<ServerModule> createServerModules() {
//                return Arrays.asList(
//                        new DiscoveryModule(webServer, getLogging()),
//                        new RESTApiModule(webServer, database, configurator.configuration(), getLogging()),
//                        new ThirdPartyJAXRSModule(webServer, configurator, getLogging(), this));
//            }
//        };
        return new WrappingNeoServer((GraphDatabaseAPI) db, configurator);
    }
}
