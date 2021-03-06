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
import com.ldbc.snb.interactive.neo4j.TestUtils;
import com.ldbc.snb.interactive.neo4j.load.LdbcSnbNeo4jImporter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

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

public class IntegrationValidationTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String CSV_DIR = TestUtils.getResource("/test_csv_files").getAbsolutePath();

    private static void buildGraph(String dbDirPath, String csvDir) throws IOException {
        String csvFilesDirPath = csvDir + "/";
        String importerConfigPath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter importer = new LdbcSnbNeo4jImporter();
        importer.load(dbDirPath, csvFilesDirPath, importerConfigPath);
    }

    @Test
    public void shouldValidateAllImplementationsUsingValidationParametersCreatedByEmbeddedCypherImplementation() throws IOException, DriverConfigurationException, ClientException {
        File dbDir = temporaryFolder.newFolder();
        // TODO uncomment to use public validation set
//        String csvDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_interactive_validation/csv_files/").getAbsolutePath();
        String csvDir = CSV_DIR;
        buildGraph(dbDir.getAbsolutePath(), csvDir);

        /*
        CREATE VALIDATION PARAMETERS FOR USE IN VALIDATING OTHER IMPLEMENTATIONS
         */

        // TODO uncomment to use public validation set
//        File validationParametersFile = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_interactive_validation/validation_params.csv");
        File validationParametersFile = temporaryFolder.newFile();
        int validationSetSize = 100;

        assertThat(validationParametersFile.length() == 0, is(true));

        // TODO uncomment to use public validation set
        long operationCount = 100000;
//        long operationCount = 100;
        int threadCount = 4;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
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
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<String, String>(),
                "LDBC-SNB",
                Neo4jDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationCreationParams,
                databaseValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        // TODO uncomment to use public validation set
//        File ldbcSnbInteractiveReadOnlyConfigurationFile = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_interactive_validation/ldbc_driver_validation_config.properties");
//        Properties ldbcSnbInteractiveReadOnlyConfigurationProperties = new Properties();
//        ldbcSnbInteractiveReadOnlyConfigurationProperties.load(new FileInputStream(ldbcSnbInteractiveReadOnlyConfigurationFile));
//        Map<String, String> ldbcSnbInteractiveReadOnlyConfiguration = MapUtils.propertiesToMap(ldbcSnbInteractiveReadOnlyConfigurationProperties);
        Map<String, String> ldbcSnbInteractiveReadOnlyConfiguration = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(ldbcSnbInteractiveReadOnlyConfiguration);

        Map<String, String> neo4jDbConfiguration = new HashMap<>();
        neo4jDbConfiguration.put(Neo4jDb.CONFIG_PATH_KEY, TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_PATH_KEY, dbDir.getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER);

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        Map<String, String> additionalParameters = new HashMap<>();
        // TODO uncomment to use public validation set
//        String substitutionParametersDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_interactive_validation/params_files/").getAbsolutePath();
        String substitutionParametersDir = csvDir;
        additionalParameters.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, substitutionParametersDir);
        additionalParameters.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(operationCount));
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
    }
}
