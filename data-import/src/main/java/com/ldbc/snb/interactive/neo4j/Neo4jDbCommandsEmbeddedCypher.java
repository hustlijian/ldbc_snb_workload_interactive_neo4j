package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.snb.interactive.neo4j.interactive.embedded_cypher.*;
import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Iterator;

public class Neo4jDbCommandsEmbeddedCypher extends Neo4jDbCommands {
    private static Logger logger = Logger.getLogger(Neo4jDbCommandsEmbeddedCypher.class);
    final private String dbPath;
    final private String configPath;
    private Neo4jConnectionState dbConnectionState;

    public Neo4jDbCommandsEmbeddedCypher(String dbPath, String configPath) {
        this.dbPath = dbPath;
        this.configPath = configPath;
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

        ExecutionEngine queryEngine = new ExecutionEngine(db);
        dbConnectionState = new Neo4jConnectionState(db, queryEngine, null, null);
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
        db.registerOperationHandler(LdbcQuery1.class, LdbcQuery1HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery2.class, LdbcQuery2HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery3.class, LdbcQuery3HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery4.class, LdbcQuery4HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery5.class, LdbcQuery5HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery6.class, LdbcQuery6HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery7.class, LdbcQuery7HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery8.class, LdbcQuery8HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery9.class, LdbcQuery9HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery10.class, LdbcQuery10HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery11.class, LdbcQuery11HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery12.class, LdbcQuery12HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery13.class, LdbcQuery13HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery14.class, LdbcQuery14HandlerEmbeddedCypher.class);
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
