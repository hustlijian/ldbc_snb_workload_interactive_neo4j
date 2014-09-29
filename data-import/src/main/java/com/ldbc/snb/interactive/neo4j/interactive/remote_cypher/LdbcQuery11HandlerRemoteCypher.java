package com.ldbc.snb.interactive.neo4j.interactive.remote_cypher;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.snb.interactive.neo4j.Neo4jConnectionState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class LdbcQuery11HandlerRemoteCypher extends OperationHandler<LdbcQuery11> {
    private static final Neo4jQuery11RemoteCypher query = new Neo4jQuery11RemoteCypher();

    @Override
    protected OperationResultReport executeOperation(LdbcQuery11 operation) throws DbException {
        Connection connection;
        try {
            connection = ((Neo4jConnectionState) dbConnectionState()).connection();
        } catch (SQLException e) {
            throw new DbException("Error while getting connection", e);
        }
        List<LdbcQuery11Result> result = null;
        int resultCode = 0;
        try {
            result = ImmutableList.copyOf(query.execute(connection, operation));
        } catch (Throwable e) {
            resultCode = 1;
        }
        return operation.buildResult(resultCode, result);
    }
}
