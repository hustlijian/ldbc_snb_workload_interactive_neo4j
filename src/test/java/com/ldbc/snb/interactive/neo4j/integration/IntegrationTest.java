package com.ldbc.snb.interactive.neo4j.integration;

import com.ldbc.driver.Client;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.snb.interactive.neo4j.TestUtils;
import com.ldbc.snb.interactive.neo4j.Neo4jDb;
import com.ldbc.snb.interactive.neo4j.load.LdbcSnbNeo4jImporter;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class IntegrationTest {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private static void buildGraph(String dbDirPath) throws IOException {
        String csvFilesDirPath = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String importerConfigPath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter importer = new LdbcSnbNeo4jImporter(dbDirPath, csvFilesDirPath, importerConfigPath);
        importer.load();
    }

    @Test
    public void shouldRunEmbeddedStepsTransactionalWorkloadWithoutThrowingException() throws ClientException, IOException, DriverConfigurationException {
        File dbDir = testFolder.newFile();
        buildGraph(dbDir.getAbsolutePath());
        File resultsFile = testFolder.newFile();
        assertThat(resultsFile.length() == 0, is(true));

        long operationCount = 10;
        int threadCount = 2;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = resultsFile.getAbsolutePath();
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams = null;
        String databaseValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<String, String>(),
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

        Map<String, String> ldbcSnbInteractiveReadOnlyConfiguration = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(ldbcSnbInteractiveReadOnlyConfiguration);

        Map<String, String> neo4jDbConfiguration = new HashMap<>();
        neo4jDbConfiguration.put(Neo4jDb.CONFIG_PATH_KEY, TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_PATH_KEY, dbDir.getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_STEPS);

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/test_csv_files/").getAbsolutePath());
        additionalParameters.put(ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, resultsFile.getAbsolutePath());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParameters);

        TimeSource timeSource = new SystemTimeSource();
        Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
        ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
        Client client = new Client(controlService, timeSource);
        client.start();

        assertThat(resultsFile.length() == 0, is(false));
    }

    @Test
    public void shouldRunEmbeddedCypherTransactionalWorkloadWithoutThrowingException() throws ClientException, IOException, DriverConfigurationException {
        File dbDir = testFolder.newFile();
        buildGraph(dbDir.getAbsolutePath());
        File resultsFile = testFolder.newFile();
        assertThat(resultsFile.length() == 0, is(true));

        long operationCount = 10;
        int threadCount = 2;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = resultsFile.getAbsolutePath();
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams = null;
        String databaseValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<String, String>(),
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

        Map<String, String> ldbcSnbInteractiveReadOnlyConfiguration = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(ldbcSnbInteractiveReadOnlyConfiguration);

        Map<String, String> neo4jDbConfiguration = new HashMap<>();
        neo4jDbConfiguration.put(Neo4jDb.CONFIG_PATH_KEY, TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_PATH_KEY, dbDir.getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER);

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/test_csv_files/").getAbsolutePath());
        additionalParameters.put(ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, resultsFile.getAbsolutePath());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParameters);

        TimeSource timeSource = new SystemTimeSource();
        Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
        ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
        Client client = new Client(controlService, timeSource);
        client.start();
        assertThat(resultsFile.length() == 0, is(false));
    }

    @Test
    public void shouldRunLoadWorkloadWithMainWithoutThrowingException() throws ClientException, IOException {
        File dbDir = testFolder.newFile();
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter.main(new String[]{dbDir.getAbsolutePath(), csvFilesDir, configFilePath});
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir.getAbsolutePath());
        db.shutdown();
    }

    @Test
    public void shouldRunLoadWorkloadUsingConstructorWithoutThrowingException() throws ClientException, IOException {
        File dbDir = testFolder.newFile();
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter ldbcSnbNeo4jImporter = new LdbcSnbNeo4jImporter(dbDir.getAbsolutePath(), csvFilesDir, configFilePath);
        ldbcSnbNeo4jImporter.load();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir.getAbsolutePath());
        db.shutdown();
    }
}
