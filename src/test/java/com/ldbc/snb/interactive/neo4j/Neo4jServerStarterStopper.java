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
import java.util.concurrent.atomic.AtomicBoolean;

public class Neo4jServerStarterStopper {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 7474;
        String path = "/tmp/neodb";
        FileUtils.deleteDirectory(new File(path));
        TestGraph.QueryGraphMaker queryGraphMaker = new TestGraph.Query5GraphMaker();
        Neo4jServerStarterStopper neo4jServerStarterStopper = Neo4jServerStarterStopper.fromQueryGraphMaker(queryGraphMaker, path, port);
        neo4jServerStarterStopper.startServer();
    }

    public static Neo4jServerStarterStopper fromQueryGraphMaker(TestGraph.QueryGraphMaker queryGraphMaker, String path, int port) throws IOException {
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

        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(path)
                .loadPropertiesFromFile(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath())
                .newGraphDatabase();

        return new Neo4jServerStarterStopper(db, port);
    }

    public static Neo4jServerStarterStopper fromPath(String path, int port) throws IOException {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(path)
                .loadPropertiesFromFile(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath())
                .newGraphDatabase();
        return new Neo4jServerStarterStopper(db, port);
    }

    public static Neo4jServerStarterStopper fromDb(GraphDatabaseService db, int port) throws IOException {
        return new Neo4jServerStarterStopper(db, port);
    }

    private final Neo4jServerStarterThread neo4jServerStarterThread;

    private Neo4jServerStarterStopper(GraphDatabaseService db, int port) {
        this.neo4jServerStarterThread = new Neo4jServerStarterThread(db, port);
    }

    public void startServer() throws IOException, InterruptedException {
        neo4jServerStarterThread.start();
        while (false == neo4jServerStarterThread.hasStarted()) {
            Thread.sleep(500);
        }
    }

    public void stopServer() {
        neo4jServerStarterThread.stopServer();
    }

    private class Neo4jServerStarterThread extends Thread {
        private final GraphDatabaseService db;
        private final AtomicBoolean continueRunning = new AtomicBoolean(true);
        private final int port;
        private final AtomicBoolean hasStarted = new AtomicBoolean(false);

        private Neo4jServerStarterThread(GraphDatabaseService db, int port) {
            this.db = db;
            this.port = port;
        }

        @Override
        public void run() {
            Configurator configurator = new ServerConfigurator((GraphDatabaseAPI) db);
            configurator.configuration().addProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, port);
            WrappingNeoServer s = new WrappingNeoServer((GraphDatabaseAPI) db, configurator);
            s.start();

            hasStarted.set(true);

            try {
                while (continueRunning.get()) {
                    Thread.sleep(500);
                }
            } catch (InterruptedException e) {
                s.start();
            }
        }

        public boolean hasStarted() {
            return hasStarted.get();
        }

        public void stopServer() {
            continueRunning.set(false);
        }
    }

}
