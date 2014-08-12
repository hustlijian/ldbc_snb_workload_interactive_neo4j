package com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.sql.Connection;
import java.util.List;

public class LdbcQuery2HandlerRemoteCypher extends OperationHandler<LdbcQuery2> {
    @Override
    protected OperationResultReport executeOperation(LdbcQuery2 operation) throws DbException {
        // TODO this needs to be in DB
        Connection connection = null;

        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        List<LdbcQuery2Result> result;
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(new Neo4jQuery2RemoteCypher().execute(connection, operation));
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
