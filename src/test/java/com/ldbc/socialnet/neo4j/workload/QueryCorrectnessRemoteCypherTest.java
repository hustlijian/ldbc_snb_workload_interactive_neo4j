package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery;
import com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher.Neo4jQuery1RemoteCypher;
import com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher.Neo4jQuery2RemoteCypher;
import org.junit.Ignore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class QueryCorrectnessRemoteCypherTest extends QueryCorrectnessTest {

    // jdbc:neo4j://<host>:<port>/, e.g. jdbc:neo4j://localhost:7474/
    // jdbc:neo4j:file:/path/to/db, e.g. jdbc:neo4j:file:/home/user/neo/graph.db
    private Connection getConnection(String path) throws DbException {
        try {
            return DriverManager.getConnection("jdbc:neo4j:file:" + path);
        } catch (SQLException e) {
            throw new DbException("Could not create database connection", e);
        }
    }

    private <OPERATION_RESULT, OPERATION extends Operation<List<OPERATION_RESULT>>> Iterator<OPERATION_RESULT> executeQuery(OPERATION operation,
                                                                                                                            Neo4jQuery<OPERATION, OPERATION_RESULT, Connection> query,
                                                                                                                            String path) throws DbException {
        // TODO uncomment to print query
        System.out.println(operation.toString() + "\n" + query.description() + "\n");
        Connection connection = getConnection(path);
        List<OPERATION_RESULT> results = ImmutableList.copyOf(query.execute(connection, operation));
        // make sure list is not lazy
        results.size();
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DbException("Error closing connection", e);
        }
        return results.iterator();
    }

    @Override
    public Iterator<LdbcQuery1Result> neo4jQuery1Impl(String path, LdbcQuery1 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery1RemoteCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery2Result> neo4jQuery2Impl(String path, LdbcQuery2 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery2RemoteCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery3Result> neo4jQuery3Impl(String path, LdbcQuery3 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery4Result> neo4jQuery4Impl(String path, LdbcQuery4 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery5Result> neo4jQuery5Impl(String path, LdbcQuery5 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery6Result> neo4jQuery6Impl(String path, LdbcQuery6 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery7Result> neo4jQuery7Impl(String path, LdbcQuery7 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery8Result> neo4jQuery8Impl(String path, LdbcQuery8 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery9Result> neo4jQuery9Impl(String path, LdbcQuery9 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery10Result> neo4jQuery10Impl(String path, LdbcQuery10 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery11Result> neo4jQuery11Impl(String path, LdbcQuery11 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery12Result> neo4jQuery12Impl(String path, LdbcQuery12 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery13Result> neo4jQuery13Impl(String path, LdbcQuery13 operation) throws DbException {
        return null;
    }

    @Override
    public Iterator<LdbcQuery14Result> neo4jQuery14Impl(String path, LdbcQuery14 operation) throws DbException {
        return null;
    }
}