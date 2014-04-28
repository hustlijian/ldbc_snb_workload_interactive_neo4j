package com.ldbc.socialnet.neo4j.integration;

import com.ldbc.driver.Client;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload;
import com.ldbc.socialnet.neo4j.workload.TestGraph;
import com.ldbc.socialnet.workload.neo4j.Neo4jDb;
import com.ldbc.socialnet.workload.neo4j.load.LdbcSocialNeworkNeo4jImporter;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IntegrationTest {
    public static String dbDir = "tempDb";

    @BeforeClass
    public static void openDb() throws IOException {
        FileUtils.deleteDirectory(new File(dbDir));
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir);
        ExecutionEngine queryEngine = new ExecutionEngine(db);

        buildGraph(db, queryEngine);
        db.shutdown();
    }

    @AfterClass
    public static void closeDb() throws IOException {
        FileUtils.deleteDirectory(new File(dbDir));
    }

    private static void buildGraph(GraphDatabaseService db, ExecutionEngine engine) {
        try (Transaction tx = db.beginTx()) {
            engine.execute(TestGraph.Creator.createGraphQuery(), TestGraph.Creator.createGraphQueryParams());
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        try (Transaction tx = db.beginTx()) {
            for (String createIndexQuery : TestGraph.Creator.createIndexQueries()) {
                engine.execute(createIndexQuery);
            }
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void shouldRunEmbeddedStepsTransactionalWorkloadWithoutThrowingException() throws ClientException {
        boolean exceptionThrown = false;
        assertThat(new File("test_results.json").exists(), is(false));
        try {
            long operationCount = 10;
            int threadCount = 1;
            boolean showStatus = true;
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            String resultFilePath = null;
            Double timeCompressionRatio = 1.0;
            Duration gctDeltaDuration = Duration.fromSeconds(10);
            List<String> peerIds = new ArrayList<>();
            Duration toleratedExecutionDelay = Duration.fromSeconds(1);

            Map<String, String> userParams = new HashMap<String, String>();
            userParams.put(LdbcInteractiveWorkload.UPDATE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
            userParams.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, TestUtils.getResource("/parameters.json").getAbsolutePath());
            userParams.put(Neo4jDb.PATH_KEY, dbDir);
            userParams.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_STEPS);

            userParams.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, Long.toString(Duration.fromMilli(10).asMilli()));
            userParams.put(LdbcInteractiveWorkload.QUERY_1_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_2_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_3_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_4_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_5_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_6_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_7_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_8_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_9_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_10_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_11_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_12_KEY, "0");

            ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                    userParams,
                    Neo4jDb.class.getName(),
                    LdbcInteractiveWorkload.class.getName(),
                    operationCount,
                    threadCount,
                    showStatus,
                    timeUnit,
                    resultFilePath,
                    timeCompressionRatio,
                    gctDeltaDuration,
                    peerIds,
                    toleratedExecutionDelay);

            Time workloadStartTime = Time.now().plus(Duration.fromSeconds(1));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, params);
            Client client = new Client(controlService);
            client.start();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
        assertThat(new File("test_results.json").exists(), is(false));
    }

    @Test
    public void shouldRunEmbeddedCypherTransactionalWorkloadWithoutThrowingException() throws ClientException {
        boolean exceptionThrown = false;
        assertThat(new File("test_results.json").exists(), is(false));
        try {
            long operationCount = 10;
            int threadCount = 1;
            boolean showStatus = true;
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            String resultFilePath = "test_results.json";
            Double timeCompressionRatio = 1.0;
            Duration gctDeltaDuration = Duration.fromSeconds(1000);
            List<String> peerIds = new ArrayList<>();
            Duration toleratedExecutionDelay = Duration.fromSeconds(100);

            Map<String, String> userParams = new HashMap<String, String>();
            userParams.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, TestUtils.getResource("/parameters.json").getAbsolutePath());
            userParams.put(Neo4jDb.PATH_KEY, dbDir);
            userParams.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER);

            userParams.put(LdbcInteractiveWorkload.UPDATE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
            userParams.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, Long.toString(Duration.fromMilli(10).asMilli()));
            userParams.put(LdbcInteractiveWorkload.QUERY_1_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_2_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_3_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_4_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_5_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_6_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_7_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.QUERY_8_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_9_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_10_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_11_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.QUERY_12_KEY, "0");

            ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                    userParams,
                    Neo4jDb.class.getName(),
                    LdbcInteractiveWorkload.class.getName(),
                    operationCount,
                    threadCount,
                    showStatus,
                    timeUnit,
                    resultFilePath,
                    timeCompressionRatio,
                    gctDeltaDuration,
                    peerIds,
                    toleratedExecutionDelay);

            Time workloadStartTime = Time.now().plus(Duration.fromSeconds(1));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, params);
            Client client = new Client(controlService);
            client.start();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
        assertThat(new File("test_results.json").exists(), is(true));
        assertThat(new File("test_results.json").delete(), is(true));
        assertThat(new File("test_results.json").exists(), is(false));
    }

    @Test
    public void shouldLoadParametersFromFileInsteadOfCommandLine() {
        boolean exceptionThrown = false;
        assertThat(new File("test_results.json").exists(), is(false));
        try {
            String neo4jLdbcSocnetInteractiveTestPropertiesPath =
                    TestUtils.getResource("/neo4j_ldbc_socnet_interactive_test.properties").getAbsolutePath();
            String ldbcSocnetInteractiveTestPropertiesPath =
                    new File("ldbc_driver/workloads/ldbc/socnet/interactive/ldbc_socnet_interactive.properties").getAbsolutePath();
            String ldbcDriverTestPropertiesPath =
                    new File("ldbc_driver/src/main/resources/ldbc_driver_default.properties").getAbsolutePath();

            assertThat(new File(neo4jLdbcSocnetInteractiveTestPropertiesPath).exists(), is(true));
            assertThat(new File(ldbcSocnetInteractiveTestPropertiesPath).exists(), is(true));
            assertThat(new File(ldbcDriverTestPropertiesPath).exists(), is(true));

            ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                    "-P", neo4jLdbcSocnetInteractiveTestPropertiesPath,
                    "-P", ldbcSocnetInteractiveTestPropertiesPath,
                    "-P", ldbcDriverTestPropertiesPath,
                    "-p", ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_ARG, Long.toString(Duration.fromMinutes(1).asMilli())});


            Time workloadStartTime = Time.now().plus(Duration.fromSeconds(1));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, params);
            Client client = new Client(controlService);
            client.start();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
        assertThat(new File("test_results.json").exists(), is(true));
        assertThat(new File("test_results.json").delete(), is(true));
        assertThat(new File("test_results.json").exists(), is(false));
    }

    @Ignore
    // TODO should not depend on existence of a directory not created by test
    @Test
    public void shouldRunLoadWorkloadWithoutThrowingException() throws ClientException {
        boolean exceptionThrown = false;
        try {
            LdbcSocialNeworkNeo4jImporter.main(new String[]{});
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
    }
}
