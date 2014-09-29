package com.ldbc.snb.interactive.neo4j.interactive;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.snb.interactive.neo4j.TestUtils;
import com.ldbc.snb.interactive.neo4j.interactive.embedded_cypher.*;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.util.Iterator;
import java.util.List;

public class QueryCorrectnessEmbeddedCypherTest extends QueryCorrectnessTest<GraphDatabaseService> {

    private <OPERATION_RESULT, OPERATION extends Operation<List<OPERATION_RESULT>>> Iterator<OPERATION_RESULT> executeQuery(
            OPERATION operation,
            Neo4jQuery<OPERATION, OPERATION_RESULT, ExecutionEngine> query,
            GraphDatabaseService graphDatabaseService) throws DbException {
        // TODO uncomment to print query
        System.out.println(query.description() + "\n");
        ExecutionEngine engine = new ExecutionEngine(graphDatabaseService);
        List<OPERATION_RESULT> results;
        try (Transaction tx = graphDatabaseService.beginTx()) {
            results = ImmutableList.copyOf(query.execute(engine, operation));
            // try to make sure list is not lazy
            results.size();
            tx.success();
        } catch (Exception e) {
            throw new DbException("Error executing query", e);
        }
        return results.iterator();
    }

    @Override
    public GraphDatabaseService openConnection(String path) throws Exception {
        return new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(path)
                .loadPropertiesFromFile(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath())
                .newGraphDatabase();
    }

    @Override
    public void closeConnection(GraphDatabaseService graphDatabaseService) throws Exception {
        graphDatabaseService.shutdown();
    }

    @Override
    public Iterator<LdbcQuery1Result> neo4jQuery1Impl(GraphDatabaseService graphDatabaseService, LdbcQuery1 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery1EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery2Result> neo4jQuery2Impl(GraphDatabaseService graphDatabaseService, LdbcQuery2 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery2EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery3Result> neo4jQuery3Impl(GraphDatabaseService graphDatabaseService, LdbcQuery3 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery3EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery4Result> neo4jQuery4Impl(GraphDatabaseService graphDatabaseService, LdbcQuery4 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery4EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery5Result> neo4jQuery5Impl(GraphDatabaseService graphDatabaseService, LdbcQuery5 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery5EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery6Result> neo4jQuery6Impl(GraphDatabaseService graphDatabaseService, LdbcQuery6 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery6EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery7Result> neo4jQuery7Impl(GraphDatabaseService graphDatabaseService, LdbcQuery7 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery7EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery8Result> neo4jQuery8Impl(GraphDatabaseService graphDatabaseService, LdbcQuery8 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery8EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery9Result> neo4jQuery9Impl(GraphDatabaseService graphDatabaseService, LdbcQuery9 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery9EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery10Result> neo4jQuery10Impl(GraphDatabaseService graphDatabaseService, LdbcQuery10 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery10EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery11Result> neo4jQuery11Impl(GraphDatabaseService graphDatabaseService, LdbcQuery11 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery11EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery12Result> neo4jQuery12Impl(GraphDatabaseService graphDatabaseService, LdbcQuery12 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery12EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery13Result> neo4jQuery13Impl(GraphDatabaseService graphDatabaseService, LdbcQuery13 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery13EmbeddedCypher(), graphDatabaseService);
    }

    @Override
    public Iterator<LdbcQuery14Result> neo4jQuery14Impl(GraphDatabaseService graphDatabaseService, LdbcQuery14 operation) throws Exception {
        return executeQuery(operation, new Neo4jQuery14EmbeddedCypher(), graphDatabaseService);
    }
}