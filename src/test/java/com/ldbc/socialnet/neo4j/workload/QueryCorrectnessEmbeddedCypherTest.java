package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery;
import com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher.*;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryCorrectnessEmbeddedCypherTest extends QueryCorrectnessTest {

    private <OPERATION_RESULT, OPERATION extends Operation<List<OPERATION_RESULT>>> Iterator<OPERATION_RESULT> executeQuery(OPERATION operation,
                                                                                                                            Neo4jQuery<OPERATION, OPERATION_RESULT, ExecutionEngine> query,
                                                                                                                            String path) throws DbException {
        // TODO uncomment to print query
        System.out.println(operation.toString() + "\n" + query.description() + "\n");
        Map dbRunConfig;
        try {
            dbRunConfig = Utils.loadConfig(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
        } catch (IOException e) {
            throw new DbException("Unable to load database properties", e);
        }
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path).setConfig(dbRunConfig).newGraphDatabase();
        ExecutionEngine engine = new ExecutionEngine(db);
        List<OPERATION_RESULT> results;
        try (Transaction tx = db.beginTx()) {
            results = ImmutableList.copyOf(query.execute(engine, operation));
            // make sure list is not lazy
            results.size();
            tx.success();
        } catch (Exception e) {
            throw new DbException("Error executing query", e);
        }
        db.shutdown();
        return results.iterator();
    }

    @Override
    public Iterator<LdbcQuery1Result> neo4jQuery1Impl(String path, LdbcQuery1 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery1EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery2Result> neo4jQuery2Impl(String path, LdbcQuery2 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery2EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery3Result> neo4jQuery3Impl(String path, LdbcQuery3 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery3EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery4Result> neo4jQuery4Impl(String path, LdbcQuery4 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery4EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery5Result> neo4jQuery5Impl(String path, LdbcQuery5 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery5EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery6Result> neo4jQuery6Impl(String path, LdbcQuery6 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery6EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery7Result> neo4jQuery7Impl(String path, LdbcQuery7 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery7EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery8Result> neo4jQuery8Impl(String path, LdbcQuery8 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery8EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery9Result> neo4jQuery9Impl(String path, LdbcQuery9 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery9EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery10Result> neo4jQuery10Impl(String path, LdbcQuery10 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery10EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery11Result> neo4jQuery11Impl(String path, LdbcQuery11 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery11EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery12Result> neo4jQuery12Impl(String path, LdbcQuery12 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery12EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery13Result> neo4jQuery13Impl(String path, LdbcQuery13 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery13EmbeddedCypher(), path);
    }

    @Override
    public Iterator<LdbcQuery14Result> neo4jQuery14Impl(String path, LdbcQuery14 operation) throws DbException {
        return executeQuery(operation, new Neo4jQuery14EmbeddedCypher(), path);
    }
}