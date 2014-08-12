package com.ldbc.socialnet.neo4j.integration;

import com.ldbc.driver.Client;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.socialnet.neo4j.TestUtils;
import com.ldbc.socialnet.workload.neo4j.Neo4jDb;
import com.ldbc.socialnet.workload.neo4j.load.LdbcSocialNeworkNeo4jImporter;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

// TODO unignore
@Ignore
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
            Map<String, String> userParams = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();

            userParams.put(Neo4jDb.DB_PATH_KEY, dbDir);
            userParams.put(Neo4jDb.CONFIG_PATH_KEY, TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
            userParams.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER);

            long operationCount = 10;
            int threadCount = 1;
            Duration statusDisplayInterval = Duration.fromSeconds(1);
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            String resultFilePath = null;
            Double timeCompressionRatio = 1.0;
            Set<String> peerIds = new HashSet<>();
            Duration toleratedExecutionDelay = Duration.fromMinutes(1);
            Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams = null;
            String databaseValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            Duration spinnerSleepDuration = Duration.fromMilli(0);
            boolean printHelp = false;
            ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                    userParams,
                    Neo4jDb.class.getName(),
                    LdbcSnbInteractiveWorkload.class.getName(),
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultFilePath,
                    timeCompressionRatio,
                    windowedExecutionWindowDuration,
                    peerIds,
                    toleratedExecutionDelay,
                    validationCreationParams,
                    databaseValidationFilePath,
                    validateWorkload,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp);

            TimeSource timeSource = new SystemTimeSource();
            Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, params);
            Client client = new Client(controlService, timeSource);
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
            Map<String, String> userParams = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();

            userParams.put(Neo4jDb.DB_PATH_KEY, dbDir);
            String configPath = TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath();
            userParams.put(Neo4jDb.CONFIG_PATH_KEY, configPath);
            userParams.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER);

            long operationCount = 10;
            int threadCount = 1;
            Duration statusDisplayInterval = Duration.fromSeconds(1);
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            String resultFilePath = null;
            Double timeCompressionRatio = 1.0;
            Set<String> peerIds = new HashSet<>();
            Duration toleratedExecutionDelay = Duration.fromMinutes(1);
            Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams = null;
            String databaseValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            Duration spinnerSleepDuration = Duration.fromMilli(0);
            boolean printHelp = false;
            ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                    userParams,
                    Neo4jDb.class.getName(),
                    LdbcSnbInteractiveWorkload.class.getName(),
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultFilePath,
                    timeCompressionRatio,
                    windowedExecutionWindowDuration,
                    peerIds,
                    toleratedExecutionDelay,
                    validationCreationParams,
                    databaseValidationFilePath,
                    validateWorkload,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp);

            TimeSource timeSource = new SystemTimeSource();
            Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, params);
            Client client = new Client(controlService, timeSource);
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


            TimeSource timeSource = new SystemTimeSource();
            Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, params);
            Client client = new Client(controlService, timeSource);
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
