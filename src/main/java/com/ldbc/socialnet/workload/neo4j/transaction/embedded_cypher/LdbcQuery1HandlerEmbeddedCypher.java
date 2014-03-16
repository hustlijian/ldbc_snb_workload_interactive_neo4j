package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery1;
import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class LdbcQuery1HandlerEmbeddedCypher extends OperationHandler<LdbcQuery1> {
    private final static Logger logger = Logger.getLogger(LdbcQuery1HandlerEmbeddedCypher.class);

    @Override
    protected OperationResult executeOperation(LdbcQuery1 operation) throws DbException {
        ExecutionEngine engine = ((Neo4jConnectionStateEmbedded) dbConnectionState()).executionEngine();
        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        Neo4jQuery1 query1 = new Neo4jQuery1EmbeddedCypher();
        List<LdbcQuery1Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(query1.execute(db, engine, operation));
            tx.success();
        } catch (Exception e) {
            logger.error(String.format("Error executing query\n%s", ConcurrentErrorReporter.stackTraceToString(e)));
            resultCode = -1;
        }

        return operation.buildResult(resultCode, result);
    }
}
