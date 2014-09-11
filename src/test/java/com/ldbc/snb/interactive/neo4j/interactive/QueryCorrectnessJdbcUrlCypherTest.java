package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.snb.interactive.neo4j.Neo4jServerHelper;
import com.ldbc.snb.interactive.neo4j.TestUtils;
import com.ldbc.snb.interactive.neo4j.interactive.remote_cypher.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.server.WrappingNeoServer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.List;

public class QueryCorrectnessJdbcUrlCypherTest extends QueryCorrectnessTest<JdbcUrlConnectionState> {
    private <OPERATION_RESULT, OPERATION extends Operation<List<OPERATION_RESULT>>> Iterator<OPERATION_RESULT> executeQuery(
            OPERATION operation,
            Neo4jQuery<OPERATION, OPERATION_RESULT, Connection> query,
            Connection connection) throws DbException {
        // TODO uncomment to print query
        System.out.println(operation.toString() + "\n" + query.description() + "\n");
        return query.execute(connection, operation);
    }

    @Override
    public JdbcUrlConnectionState openConnection(String path) throws Exception {
        GraphDatabaseService db;
        WrappingNeoServer wrappingNeoServer;
        Connection connection;
        try {
            db = new GraphDatabaseFactory()
                    .newEmbeddedDatabaseBuilder(path)
                    .loadPropertiesFromFile(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath())
                    .newGraphDatabase();
            int serverPort = Neo4jServerHelper.nextFreePort();
            wrappingNeoServer = Neo4jServerHelper.fromDb(db, serverPort);
            wrappingNeoServer.start();
            connection = DriverManager.getConnection("jdbc:neo4j://localhost:" + serverPort);
        } catch (Throwable e) {
            throw new DbException("Could not create database connection", e);
        }
        return new JdbcUrlConnectionState(connection, wrappingNeoServer, db);
    }

    @Override
    public void closeConnection(JdbcUrlConnectionState connection) throws Exception {
        connection.connection().close();
        connection.server().stop();
        connection.db().shutdown();
    }

    @Override
    public Iterator<LdbcQuery1Result> neo4jQuery1Impl(JdbcUrlConnectionState connection, LdbcQuery1 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery1RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery2Result> neo4jQuery2Impl(JdbcUrlConnectionState connection, LdbcQuery2 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery2RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery3Result> neo4jQuery3Impl(JdbcUrlConnectionState connection, LdbcQuery3 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery3RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery4Result> neo4jQuery4Impl(JdbcUrlConnectionState connection, LdbcQuery4 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery4RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery5Result> neo4jQuery5Impl(JdbcUrlConnectionState connection, LdbcQuery5 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery5RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery6Result> neo4jQuery6Impl(JdbcUrlConnectionState connection, LdbcQuery6 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery6RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery7Result> neo4jQuery7Impl(JdbcUrlConnectionState connection, LdbcQuery7 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery7RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery8Result> neo4jQuery8Impl(JdbcUrlConnectionState connection, LdbcQuery8 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery8RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery9Result> neo4jQuery9Impl(JdbcUrlConnectionState connection, LdbcQuery9 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery9RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery10Result> neo4jQuery10Impl(JdbcUrlConnectionState connection, LdbcQuery10 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery10RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery11Result> neo4jQuery11Impl(JdbcUrlConnectionState connection, LdbcQuery11 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery11RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery12Result> neo4jQuery12Impl(JdbcUrlConnectionState connection, LdbcQuery12 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery12RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery13Result> neo4jQuery13Impl(JdbcUrlConnectionState connection, LdbcQuery13 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery13RemoteCypher(), connection.connection());
    }

    @Override
    public Iterator<LdbcQuery14Result> neo4jQuery14Impl(JdbcUrlConnectionState connection, LdbcQuery14 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery14RemoteCypher(), connection.connection());
    }
}