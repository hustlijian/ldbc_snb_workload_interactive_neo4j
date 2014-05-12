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
import com.ldbc.socialnet.workload.neo4j.Neo4jDb;
import com.ldbc.socialnet.workload.neo4j.load.LdbcSocialNeworkNeo4jImporter;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
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
        FileUtils.forceMkdir(new File(dbDir));
        buildGraph(new File(dbDir).getAbsolutePath());
    }

    @AfterClass
    public static void closeDb() throws IOException {
        FileUtils.deleteDirectory(new File(dbDir));
    }

    private static void buildGraph(String dbDirPath) throws IOException {
        String csvFilesDirPath = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String importerConfigPath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSocialNeworkNeo4jImporter importer = new LdbcSocialNeworkNeo4jImporter(dbDirPath, csvFilesDirPath, importerConfigPath);
        importer.load();
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
            Duration toleratedExecutionDelay = Duration.fromMinutes(1);

            Map<String, String> userParams = new HashMap<>();
            userParams.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
            userParams.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, TestUtils.getResource("/parameters.json").getAbsolutePath());
            userParams.put(Neo4jDb.DB_PATH_KEY, dbDir);
            userParams.put(Neo4jDb.CONFIG_PATH_KEY, TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
            userParams.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER);

            userParams.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, Long.toString(Duration.fromMilli(10).asMilli()));
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_1_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_2_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_3_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_4_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_5_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_6_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_7_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_8_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_9_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_10_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_11_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_12_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_13_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_14_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "false");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "false");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "false");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "false");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "false");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "false");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "false");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "false");
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

            Map<String, String> userParams = new HashMap<>();
            userParams.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, TestUtils.getResource("/parameters.json").getAbsolutePath());
            userParams.put(Neo4jDb.DB_PATH_KEY, dbDir);
            String configPath = TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath();
            userParams.put(Neo4jDb.CONFIG_PATH_KEY, configPath);
            userParams.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER);

            userParams.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
            userParams.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
            userParams.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, Long.toString(Duration.fromMilli(10).asMilli()));
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_1_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_2_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_3_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_4_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_5_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_6_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_7_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_8_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_9_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_10_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_11_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_12_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_13_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.READ_OPERATION_14_KEY, "1");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "true");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "true");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "true");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "true");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "true");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "true");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "true");
            userParams.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "true");

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

            String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
            ConsoleAndFileDriverConfiguration params = ConsoleAndFileDriverConfiguration.fromArgs(new String[]{
                    "-p", Neo4jDb.CONFIG_PATH_KEY, configFilePath,
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


    @Test
    public void shouldRunLoadWorkloadWithMainWithoutThrowingException() throws ClientException {
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String tempDbDir = "tempImportDbDir";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        boolean exceptionThrown = false;
        try {
            FileUtils.deleteDirectory(new File(tempDbDir));
            LdbcSocialNeworkNeo4jImporter.main(new String[]{tempDbDir, csvFilesDir, configFilePath});
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(tempDbDir);
            db.shutdown();
            FileUtils.deleteDirectory(new File(tempDbDir));
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }

        assertThat(exceptionThrown, is(false));
    }

    @Test
    public void shouldRunLoadWorkloadUsingConstructorWithoutThrowingException() throws ClientException {
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String tempDbDir = "tempImportDbDir";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        boolean exceptionThrown = false;
        try {
            FileUtils.deleteDirectory(new File(tempDbDir));
            LdbcSocialNeworkNeo4jImporter ldbcSocialNeworkNeo4jImporter = new LdbcSocialNeworkNeo4jImporter(tempDbDir, csvFilesDir, configFilePath);
            ldbcSocialNeworkNeo4jImporter.load();
            GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(tempDbDir);
            db.shutdown();
            FileUtils.deleteDirectory(new File(tempDbDir));
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }

        assertThat(exceptionThrown, is(false));
    }
}
