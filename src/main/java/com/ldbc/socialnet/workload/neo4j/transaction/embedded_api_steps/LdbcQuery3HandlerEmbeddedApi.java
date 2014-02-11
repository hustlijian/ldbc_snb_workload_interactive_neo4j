package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runner.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery3Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery3;
import org.apache.log4j.Logger;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;

public class LdbcQuery3HandlerEmbeddedApi extends OperationHandler<LdbcQuery3> {
    private final static Logger logger = Logger.getLogger(LdbcQuery3HandlerEmbeddedApi.class);

    @Override
    protected OperationResult executeOperation(LdbcQuery3 operation) throws DbException {
        ExecutionEngine engine = ((Neo4jConnectionStateEmbedded) dbConnectionState()).executionEngine();
        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        LdbcTraversers traversers = ((Neo4jConnectionStateEmbedded) dbConnectionState()).traversers();
        Neo4jQuery3 query3 = new Neo4jQuery3EmbeddedApi(traversers);
        List<LdbcQuery3Result> result = null;

        // TODO find way to do this
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(query3.execute(db, engine, operation));
            tx.success();
        } catch (Exception e) {
            logger.error(String.format("Error executing query\n%s\n%s", operation.toString(), ConcurrentErrorReporter.stackTraceToString(e)));
            resultCode = -1;
        }

        return operation.buildResult(resultCode, result);
    }
}
