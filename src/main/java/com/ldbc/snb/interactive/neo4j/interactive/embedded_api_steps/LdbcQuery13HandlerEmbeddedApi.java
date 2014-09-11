package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.snb.interactive.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class LdbcQuery13HandlerEmbeddedApi extends OperationHandler<LdbcQuery13> {
    @Override
    protected OperationResultReport executeOperation(LdbcQuery13 operation) throws DbException {
        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        LdbcTraversers traversers = ((Neo4jConnectionStateEmbedded) dbConnectionState()).traversers();
        List<LdbcQuery13Result> result;
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(new Neo4jQuery13EmbeddedApi(traversers).execute(db, operation));
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
