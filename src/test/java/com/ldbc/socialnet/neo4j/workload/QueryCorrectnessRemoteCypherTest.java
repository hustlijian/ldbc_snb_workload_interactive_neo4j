package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery;
import com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher.Neo4jQuery1RemoteCypher;
import com.ldbc.socialnet.workload.neo4j.interactive.remote_cypher.Neo4jQuery2RemoteCypher;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class QueryCorrectnessRemoteCypherTest extends QueryCorrectnessTest<Connection> {

    private <OPERATION_RESULT, OPERATION extends Operation<List<OPERATION_RESULT>>> Iterator<OPERATION_RESULT> executeQuery(
            OPERATION operation,
            Neo4jQuery<OPERATION, OPERATION_RESULT, Connection> query,
            Connection connection) throws DbException {
        // TODO uncomment to print query
        System.out.println(operation.toString() + "\n" + query.description() + "\n");
        List<OPERATION_RESULT> results = ImmutableList.copyOf(query.execute(connection, operation));
        // make sure list is not lazy
        results.size();
        return results.iterator();
    }

    // jdbc:neo4j://<host>:<port>/, e.g. jdbc:neo4j://localhost:7474/
    // jdbc:neo4j:file:/path/to/db, e.g. jdbc:neo4j:file:/home/user/neo/graph.db
    @Override
    public Connection openConnection(String path) throws Exception {
        try {
            return DriverManager.getConnection("jdbc:neo4j:file:" + path);
        } catch (SQLException e) {
            throw new DbException("Could not create database connection", e);
        }
    }

    @Override
    public void closeConnection(Connection connection) throws Exception {
        connection.close();
    }

    @Override
    public Iterator<LdbcQuery1Result> neo4jQuery1Impl(Connection connection, LdbcQuery1 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery1RemoteCypher(), connection);
    }

    @Override
    public Iterator<LdbcQuery2Result> neo4jQuery2Impl(Connection connection, LdbcQuery2 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery2RemoteCypher(), connection);
    }

    @Override
    public Iterator<LdbcQuery3Result> neo4jQuery3Impl(Connection connection, LdbcQuery3 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery4Result> neo4jQuery4Impl(Connection connection, LdbcQuery4 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery5Result> neo4jQuery5Impl(Connection connection, LdbcQuery5 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery6Result> neo4jQuery6Impl(Connection connection, LdbcQuery6 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery7Result> neo4jQuery7Impl(Connection connection, LdbcQuery7 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery8Result> neo4jQuery8Impl(Connection connection, LdbcQuery8 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery9Result> neo4jQuery9Impl(Connection connection, LdbcQuery9 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery10Result> neo4jQuery10Impl(Connection connection, LdbcQuery10 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery11Result> neo4jQuery11Impl(Connection connection, LdbcQuery11 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery12Result> neo4jQuery12Impl(Connection connection, LdbcQuery12 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery13Result> neo4jQuery13Impl(Connection connection, LdbcQuery13 operation) throws Exception {
        // TODO
        return null;
    }

    @Override
    public Iterator<LdbcQuery14Result> neo4jQuery14Impl(Connection connection, LdbcQuery14 operation) throws Exception {
        // TODO
        return null;
    }
}