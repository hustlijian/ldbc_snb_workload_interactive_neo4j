package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery6;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class LdbcQuery6HandlerEmbeddedCypher extends OperationHandler<LdbcQuery6> {
    @Override
    protected OperationResult executeOperation(LdbcQuery6 operation) throws DbException {
        ExecutionEngine engine = ((Neo4jConnectionStateEmbedded) dbConnectionState()).executionEngine();
        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        Neo4jQuery6 query6 = new Neo4jQuery6EmbeddedCypher();
        List<LdbcQuery6Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(query6.execute(db, engine, operation));
            tx.success();
        } catch (Exception e) {
            String errMsg = String.format(
                    "Error executing query\n%s\n%s",
                    operation.toString(),
                    ConcurrentErrorReporter.stackTraceToString(e));
            throw new DbException(errMsg, e);
        }

        return operation.buildResult(resultCode, result);
    }
}
