package com.ldbc.socialnet.workload.neo4j.utils;

import com.ldbc.driver.util.Tuple.Tuple2;
import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import java.util.concurrent.TimeUnit;

//import org.neo4j.cypher.CouldNotDropIndexException;

public class GraphUtils {
    private final static Logger logger = Logger.getLogger(GraphUtils.class);

    public static void createDeferredSchemaIndexesUsingBatchInserter(final BatchInserter inserter,
                                                                     Iterable<Tuple2<Label, String>> labelPropertyPairsToIndex) {
        for (Tuple2<Label, String> schemaIndex : labelPropertyPairsToIndex) {
            Label label = schemaIndex._1();
            String property = schemaIndex._2();
            inserter.createDeferredSchemaIndex(label).on(property).create();
            logger.info(String.format("Created schema index for Label[%s] Property[%s]", label, property));
        }
    }

    public static void createSchemaIndexesUsingCypher(GraphDatabaseService db, ExecutionEngine queryEngine,
                                                      Iterable<Tuple2<Label, String>> labelPropertyPairsToIndex, long msTimeout, boolean dropFirst) {
        Transaction tx = db.beginTx();
        try {
            if (dropFirst) {
                for (Tuple2<Label, String> labelPropertyPair : labelPropertyPairsToIndex) {
                    Label label = labelPropertyPair._1();
                    String property = labelPropertyPair._2();
                    try {
                        queryEngine.execute("DROP INDEX ON :" + label + "(" + property + ")");
                    } catch (Exception e)
                    // catch ( CouldNotDropIndexException e )
                    {
                        logger.info(String.format("Index does not exist on Label[%s] Property[%s] - can not drop",
                                label, property));
                    }
                }
            }

            for (Tuple2<Label, String> labelPropertyPair : labelPropertyPairsToIndex) {
                Label label = labelPropertyPair._1();
                String property = labelPropertyPair._2();
                queryEngine.execute("CREATE INDEX ON :" + label + "(" + property + ")");
            }
            db.schema().awaitIndexesOnline(msTimeout, TimeUnit.MILLISECONDS);
            tx.success();
        } catch (Exception e) {
            tx.failure();
            throw e;
        }

    }

    public static void waitForIndexesToBeOnline(GraphDatabaseService db, Iterable<Label> labelsToIndex) {
        Transaction tx = db.beginTx();
        try {
            for (Label label : labelsToIndex) {
                for (IndexDefinition indexDefinition : db.schema().getIndexes(label)) {
                    IndexState indexState = db.schema().getIndexState(indexDefinition);
                    logger.info(String.format("Schema index on Label[%s] Properties[%s] - %s",
                            indexDefinition.getLabel(), indexDefinition.getPropertyKeys().toString(), indexState));

                    if (db.schema().getIndexState(indexDefinition) == IndexState.FAILED)
                        throw new Exception(String.format(
                                "Schema index for Label[%s] Properties[%s] failed to build",
                                indexDefinition.getLabel(), indexDefinition.getPropertyKeys().toString()));

                    if (indexState != IndexState.ONLINE) {
                        logger.info(String.format("Waiting for schema indexes on Label[%s] Properties[%s] to build",
                                indexDefinition.getLabel(), indexDefinition.getPropertyKeys().toString()));
                        while (db.schema().getIndexState(indexDefinition) == IndexState.POPULATING) {
                            // do nothing
                        }
                    }
                }
            }
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static long nodeCount(GraphDatabaseService db, long transactionSize) {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at(db);
        long nodeCount = 0;
        Transaction tx = db.beginTx();
        try {
            for (Node node : globalOperations.getAllNodes()) {
                nodeCount++;
                if (nodeCount % transactionSize == 0) {
                    tx.success();
                    tx = db.beginTx();
                }
            }
            tx.success();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return nodeCount;
    }

    public static long relationshipCount(GraphDatabaseService db, long transactionSize) {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at(db);
        long relationshipCount = 0;
        Transaction tx = db.beginTx();
        try {
            for (Relationship relationship : globalOperations.getAllRelationships()) {
                relationshipCount++;
                if (relationshipCount % transactionSize == 0) {
                    tx.success();
                    tx = db.beginTx();
                }
            }
            tx.success();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return relationshipCount;
    }
}
