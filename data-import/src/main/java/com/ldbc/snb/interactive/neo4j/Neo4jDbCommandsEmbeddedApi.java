package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps.*;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Iterator;

public class Neo4jDbCommandsEmbeddedApi extends Neo4jDbCommands {
    private static Logger logger = Logger.getLogger(Neo4jDbCommandsEmbeddedApi.class);
    private final String dbPath;
    final private String configPath;
    private final LdbcTraversersType traversersType;
    private Neo4jConnectionState dbConnectionState;
    private LdbcTraversers traversers;

    public enum LdbcTraversersType {
        STEPS
    }

    public Neo4jDbCommandsEmbeddedApi(String dbPath, String configPath, LdbcTraversersType traversersType) {
        this.dbPath = dbPath;
        this.configPath = configPath;
        this.traversersType = traversersType;
    }

    @Override
    public void init(boolean doWarmup) throws DbException {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(dbPath)
                .loadPropertiesFromFile(configPath)
                .newGraphDatabase();

        if (doWarmup) {
            long nodeCount = 0;
            long relationshipCount = 0;
            final int maxTransactionSize = 10000;

            Iterator<Node> allNodes;
            try (Transaction tx = db.beginTx()) {
                allNodes = GlobalGraphOperations.at(db).getAllNodes().iterator();
                tx.success();
            }
            boolean moreNodesRemain = true;
            while (moreNodesRemain) {
                int inThisTransaction = 0;
                try (Transaction tx = db.beginTx()) {
                    while (inThisTransaction < maxTransactionSize && allNodes.hasNext()) {
                        allNodes.next();
                        nodeCount++;
                        inThisTransaction++;
                    }
                    moreNodesRemain = allNodes.hasNext();
                    tx.success();
                }
            }

            Iterator<Relationship> allRelationships;
            try (Transaction tx = db.beginTx()) {
                allRelationships = GlobalGraphOperations.at(db).getAllRelationships().iterator();
                tx.success();
            }
            boolean moreRelationshipsRemain = true;
            while (moreRelationshipsRemain) {
                int inThisTransaction = 0;
                try (Transaction tx = db.beginTx()) {
                    while (inThisTransaction < maxTransactionSize && allRelationships.hasNext()) {
                        allRelationships.next();
                        relationshipCount++;
                        inThisTransaction++;
                    }
                    moreRelationshipsRemain = allRelationships.hasNext();
                    tx.success();
                }
            }

            logger.info("- Simple database statistics -");
            logger.info("Node count: " + nodeCount);
            logger.info("Relationship count: " + relationshipCount);
        }

        switch (traversersType) {
            case STEPS:
                traversers = new LdbcTraversers(db);
                break;
            default:
                throw new RuntimeException("Unrecognized LdbcTraversersType: " + traversersType.name());
        }
        dbConnectionState = new Neo4jConnectionState(db, null, traversers, null);
        registerShutdownHook(db);
    }

    @Override
    public void cleanUp() throws DbException {
        dbConnectionState.shutdown();
    }

    @Override
    public DbConnectionState getDbConnectionState() {
        return dbConnectionState;
    }

    @Override
    public void registerHandlersWithDb(Db db) throws DbException {
        db.registerOperationHandler(LdbcQuery1.class, LdbcQuery1HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery2.class, LdbcQuery2HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery3.class, LdbcQuery3HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery4.class, LdbcQuery4HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery5.class, LdbcQuery5HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery6.class, LdbcQuery6HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery7.class, LdbcQuery7HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery8.class, LdbcQuery8HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery9.class, LdbcQuery9HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery10.class, LdbcQuery10HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery11.class, LdbcQuery11HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery12.class, LdbcQuery12HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery13.class, LdbcQuery13HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery14.class, LdbcQuery14HandlerEmbeddedApi.class);
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}
