package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery10;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class LdbcQuery10HandlerEmbeddedApi extends OperationHandler<LdbcQuery10> {
    @Override
    protected OperationResult executeOperation(LdbcQuery10 operation) throws DbException {
        ExecutionEngine engine = ((Neo4jConnectionStateEmbedded) dbConnectionState()).executionEngine();
        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        LdbcTraversers traversers = ((Neo4jConnectionStateEmbedded) dbConnectionState()).traversers();
        Neo4jQuery10 query = new Neo4jQuery10EmbeddedApi(traversers);
        List<LdbcQuery10Result> result;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(query.execute(db, engine, operation));
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
