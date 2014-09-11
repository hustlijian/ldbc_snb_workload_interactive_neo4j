package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.snb.interactive.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery4;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class LdbcQuery4HandlerEmbeddedApi extends OperationHandler<LdbcQuery4> {
    @Override
    protected OperationResultReport executeOperation(LdbcQuery4 operation) throws DbException {
        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        LdbcTraversers traversers = ((Neo4jConnectionStateEmbedded) dbConnectionState()).traversers();
        Neo4jQuery4 query4 = new Neo4jQuery4EmbeddedApi(traversers);
        List<LdbcQuery4Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(query4.execute(db, operation));
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
