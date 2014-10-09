package com.ldbc.snb.interactive.neo4j.interactive.embedded_cypher;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.snb.interactive.neo4j.Neo4jConnectionState;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class LdbcQuery7HandlerEmbeddedCypher extends OperationHandler<LdbcQuery7> {
    @Override
    protected OperationResultReport executeOperation(LdbcQuery7 operation) throws DbException {
        // TODO remove
        System.out.println("STARTED: " + operation.toString());
        ExecutionEngine engine = ((Neo4jConnectionState) dbConnectionState()).executionEngine();
        GraphDatabaseService db = ((Neo4jConnectionState) dbConnectionState()).db();
        List<LdbcQuery7Result> result;
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(new Neo4jQuery7EmbeddedCypher().execute(engine, operation));
            tx.success();
        } catch (Exception e) {
            String errMsg = String.format(
                    "Error executing query\n%s\n%s",
                    operation.toString(),
                    ConcurrentErrorReporter.stackTraceToString(e));
            throw new DbException(errMsg, e);
        }
        // TODO remove
        System.out.println("FINISHED: " + operation.toString());
        return operation.buildResult(resultCode, result);
    }
}
