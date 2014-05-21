package com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.Neo4jConnectionStateEmbedded;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery1;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.sql.*;
import java.util.List;

public class LdbcQuery1HandlerRemoteCypher extends OperationHandler<LdbcQuery1> {
    @Override
    protected OperationResult executeOperation(LdbcQuery1 operation) throws DbException {

        // TODO this needs to be in DB
        // Make sure Neo4j Driver is registered
        try {
            Class.forName("org.neo4j.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // TODO this needs to be in DB
        // Connect
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:neo4j://localhost:7474/");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        GraphDatabaseService db = ((Neo4jConnectionStateEmbedded) dbConnectionState()).db();
        List<LdbcQuery1Result> result;
        int resultCode = 0;
        try (Transaction tx = db.beginTx()) {
            result = ImmutableList.copyOf(new Neo4jQuery1RemoteCypher().execute(connection, operation));
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
