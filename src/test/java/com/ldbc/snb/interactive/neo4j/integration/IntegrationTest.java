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
import com.ldbc.snb.interactive.neo4j.Neo4jDb;
import com.ldbc.snb.interactive.neo4j.Neo4jServerHelper;
import com.ldbc.snb.interactive.neo4j.TestUtils;
import com.ldbc.snb.interactive.neo4j.load.LdbcSnbNeo4jImporter;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.server.WrappingNeoServer;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class IntegrationTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static void buildGraph(String dbDirPath) throws IOException {
        String csvFilesDirPath = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String importerConfigPath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter importer = new LdbcSnbNeo4jImporter(dbDirPath, csvFilesDirPath, importerConfigPath);
        importer.load();
    }

    @Ignore
    @Test
    public void importerShouldBeReusableParametersShouldBeGivenToLoadMethodNotToConstructor() {
        assertThat(true, is(false));
    }

    @Test
    public void shouldValidateAllImplementationUsingValidationParametersCreatedByEmbeddedCypherImplementation() throws IOException, DriverConfigurationException, ClientException {
        File dbDir = temporaryFolder.newFile();
        buildGraph(dbDir.getAbsolutePath());

        /*
        CREATE VALIDATION PARAMETERS FOR USE IN VALIDATING OTHER IMPLEMENTATIONS
         */

        File validationParametersFile = temporaryFolder.newFile();
        int validationSetSize = 100;

        assertThat(validationParametersFile.length() == 0, is(true));

        long operationCount = 10;
        int threadCount = 4;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = null;
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams =
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions(validationParametersFile.getAbsolutePath(), validationSetSize);
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
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParameters);

        TimeSource timeSource = new SystemTimeSource();
        Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
        ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
        Client client = new Client(controlService, timeSource);
        client.start();

        assertThat(validationParametersFile.length() == 0, is(false));

         /*
        VALIDATE EMBEDDED CYPHER IMPLEMENTATION AGAINST VALIDATION PARAMETERS CREATED BY EMBEDDED CYPHER IMPLEMENTATION
         */

        Map<String, String> validateDbConfigurationParameters = new HashMap<>();
        validateDbConfigurationParameters.put(ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG, validationParametersFile.getAbsolutePath());
        validateDbConfigurationParameters.put(ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG, null);
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(validateDbConfigurationParameters);

        controlService = new LocalControlService(workloadStartTime, configuration);
        client = new Client(controlService, timeSource);
        assertThat(client.databaseValidationResult(), is(nullValue()));
        client.start();
        assertThat(client.databaseValidationResult().isSuccessful(), is(true));

         /*
        VALIDATE EMBEDDED API IMPLEMENTATION AGAINST VALIDATION PARAMETERS CREATED BY EMBEDDED CYPHER IMPLEMENTATION
         */

        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_API);
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        controlService = new LocalControlService(workloadStartTime, configuration);
        client = new Client(controlService, timeSource);
        assertThat(client.databaseValidationResult(), is(nullValue()));
        client.start();
        assertThat(client.databaseValidationResult().isSuccessful(), is(true));

         /*
        VALIDATE JDBC CYPHER IMPLEMENTATION AGAINST VALIDATION PARAMETERS CREATED BY EMBEDDED CYPHER IMPLEMENTATION
         */

        int port = Neo4jServerHelper.nextFreePort();
        WrappingNeoServer wrappingNeoServer = Neo4jServerHelper.fromPath(dbDir.getAbsolutePath(), port);
        wrappingNeoServer.start();

        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_REMOTE_CYPHER);
        neo4jDbConfiguration.put(Neo4jDb.URL_KEY, "jdbc:neo4j://localhost:" + port);
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        controlService = new LocalControlService(workloadStartTime, configuration);
        client = new Client(controlService, timeSource);
        assertThat(client.databaseValidationResult(), is(nullValue()));
        client.start();
        assertThat(client.databaseValidationResult().isSuccessful(), is(true));

        wrappingNeoServer.stop();
    }

    @Test
    public void shouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedSteps() throws ClientException, IOException, DriverConfigurationException {
        File dbDir = temporaryFolder.newFile();
        buildGraph(dbDir.getAbsolutePath());
        File resultsFile = temporaryFolder.newFile();
        assertThat(resultsFile.length() == 0, is(true));

        long operationCount = 10;
        int threadCount = 4;
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
        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_API);

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/test_csv_files/").getAbsolutePath());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParameters);

        TimeSource timeSource = new SystemTimeSource();
        Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
        ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
        Client client = new Client(controlService, timeSource);
        client.start();

        assertThat(resultsFile.length() == 0, is(false));
    }

    @Test
    public void shouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedCypher() throws ClientException, IOException, DriverConfigurationException {
        File dbDir = temporaryFolder.newFile();
        buildGraph(dbDir.getAbsolutePath());
        File resultsFile = temporaryFolder.newFile();
        assertThat(resultsFile.length() == 0, is(true));

        long operationCount = 10;
        int threadCount = 4;
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
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParameters);

        TimeSource timeSource = new SystemTimeSource();
        Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
        ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
        Client client = new Client(controlService, timeSource);
        client.start();
        assertThat(resultsFile.length() == 0, is(false));
    }

    @Test
    public void shouldRunLdbcSnbInteractiveReadOnlyWorkloadWithRemoteCypher() throws ClientException, IOException, DriverConfigurationException {
        File dbDir = temporaryFolder.newFile();
        buildGraph(dbDir.getAbsolutePath());

        int port = Neo4jServerHelper.nextFreePort();
        WrappingNeoServer wrappingNeoServer = Neo4jServerHelper.fromPath(dbDir.getAbsolutePath(), port);
        wrappingNeoServer.start();

        File resultsFile = temporaryFolder.newFile();
        assertThat(resultsFile.length() == 0, is(true));

        long operationCount = 10;
        int threadCount = 4;
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
        neo4jDbConfiguration.put(Neo4jDb.URL_KEY, "jdbc:neo4j://localhost:" + port);
        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_REMOTE_CYPHER);

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/test_csv_files/").getAbsolutePath());
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParameters);

        TimeSource timeSource = new SystemTimeSource();
        Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
        ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
        Client client = new Client(controlService, timeSource);
        client.start();
        assertThat(resultsFile.length() == 0, is(false));
        wrappingNeoServer.stop();
    }


    @Test
    public void shouldLoadDatasetWithMain() throws ClientException, IOException {
        File dbDir = temporaryFolder.newFile();
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter.main(new String[]{dbDir.getAbsolutePath(), csvFilesDir, configFilePath});
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir.getAbsolutePath());
        db.shutdown();
    }

    @Test
    public void shouldLoadDatasetUsingConstructor() throws ClientException, IOException {
        File dbDir = temporaryFolder.newFile();
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter ldbcSnbNeo4jImporter = new LdbcSnbNeo4jImporter(dbDir.getAbsolutePath(), csvFilesDir, configFilePath);
        ldbcSnbNeo4jImporter.load();
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir.getAbsolutePath());
        db.shutdown();
    }
}
