package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.socialnet.neo4j.TestUtils;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery;
import com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps.*;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

// TODO unignore
@Ignore
public class QueryCorrectnessEmbeddedApiStepsTest extends QueryCorrectnessTest {

    private GraphDatabaseService getDb(String path) throws DbException {
        Map dbRunConfig;
        try {
            dbRunConfig = Utils.loadConfig(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
        } catch (IOException e) {
            throw new DbException("Unable to load database properties", e);
        }
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path).setConfig(dbRunConfig).newGraphDatabase();
    }

    private <OPERATION_RESULT, OPERATION extends Operation<List<OPERATION_RESULT>>> Iterator<OPERATION_RESULT> executeQuery(OPERATION operation,
                                                                                                                            Neo4jQuery<OPERATION, OPERATION_RESULT, GraphDatabaseService> query,
                                                                                                                            GraphDatabaseService db)
            throws DbException {
        // TODO uncomment to print query
        System.out.println(operation.toString() + "\n" + query.description() + "\n");
        List<OPERATION_RESULT> results;
        try (Transaction tx = db.beginTx()) {
            results = ImmutableList.copyOf(query.execute(db, operation));
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
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery1EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery2Result> neo4jQuery2Impl(String path, LdbcQuery2 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery2EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery3Result> neo4jQuery3Impl(String path, LdbcQuery3 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery3EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery4Result> neo4jQuery4Impl(String path, LdbcQuery4 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery4EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery5Result> neo4jQuery5Impl(String path, LdbcQuery5 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery5EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery6Result> neo4jQuery6Impl(String path, LdbcQuery6 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery6EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery7Result> neo4jQuery7Impl(String path, LdbcQuery7 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery7EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery8Result> neo4jQuery8Impl(String path, LdbcQuery8 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery8EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery9Result> neo4jQuery9Impl(String path, LdbcQuery9 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery9EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery10Result> neo4jQuery10Impl(String path, LdbcQuery10 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery10EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery11Result> neo4jQuery11Impl(String path, LdbcQuery11 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery11EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery12Result> neo4jQuery12Impl(String path, LdbcQuery12 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery12EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery13Result> neo4jQuery13Impl(String path, LdbcQuery13 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery13EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }

    @Override
    public Iterator<LdbcQuery14Result> neo4jQuery14Impl(String path, LdbcQuery14 operation) throws DbException {
        GraphDatabaseService db = getDb(path);
        return executeQuery(operation, new Neo4jQuery14EmbeddedApi(new LdbcTraversersSteps(db)), db);
    }
}
