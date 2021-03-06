package com.ldbc.snb.interactive.neo4j.integration;

import com.google.common.collect.Iterators;
import com.ldbc.driver.Client;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.CsvFileReader;
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
import static org.junit.Assert.assertThat;

public class IntegrationBenchmarkExecutionApiTest {
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
    public void shouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedSteps() throws ClientException, IOException, DriverConfigurationException {
        long operationCount = 100;
        boolean ignoreScheduledStartTimes = false;
        doShouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedSteps(ignoreScheduledStartTimes, operationCount);
    }

    @Test
    public void shouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedStepsIgnoringStartTimes() throws ClientException, IOException, DriverConfigurationException {
        long operationCount = 100;
        boolean ignoreScheduledStartTimes = true;
        doShouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedSteps(ignoreScheduledStartTimes, operationCount);
    }

    public void doShouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedSteps(boolean ignoreScheduledStartTimes, long operationCount) throws ClientException, IOException, DriverConfigurationException {
        File dbDir = temporaryFolder.newFolder();
        String csvDir = CSV_DIR;
        buildGraph(dbDir.getAbsolutePath(), csvDir);
        File resultDir = temporaryFolder.newFolder();
        assertThat(resultDir.listFiles().length, is(0));

        int threadCount = 4;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = resultDir.getAbsolutePath();
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
        boolean shouldCreateResultsLog = true;

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

        Map<String, String> ldbcSnbInteractiveReadOnlyConfiguration = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(ldbcSnbInteractiveReadOnlyConfiguration);

        Map<String, String> neo4jDbConfiguration = new HashMap<>();
        neo4jDbConfiguration.put(Neo4jDb.CONFIG_PATH_KEY, TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_PATH_KEY, dbDir.getAbsolutePath());
        neo4jDbConfiguration.put(Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_API);
        neo4jDbConfiguration.put(Neo4jDb.WARMUP_KEY, "true");

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(neo4jDbConfiguration);

        Map<String, String> additionalParameters = new HashMap<>();
        additionalParameters.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, csvDir);
        additionalParameters.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(operationCount));
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(additionalParameters);

        TimeSource timeSource = new SystemTimeSource();
        Time workloadStartTime = timeSource.now().plus(Duration.fromSeconds(1));
        ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
        Client client = new Client(controlService, timeSource);
        client.start();

        // results, configuration
        Set<String> fileNames = new HashSet<>();
        for (File file : resultDir.listFiles()) {
            fileNames.add(file.getName());
        }
        assertThat(fileNames.size(), is(3));
        assertThat(fileNames.contains(configuration.name() + ThreadedQueuedConcurrentMetricsService.RESULTS_CONFIGURATION_FILENAME_SUFFIX), is(true));
        assertThat(fileNames.contains(configuration.name() + ThreadedQueuedConcurrentMetricsService.RESULTS_METRICS_FILENAME_SUFFIX), is(true));
        assertThat(fileNames.contains(configuration.name() + ThreadedQueuedConcurrentMetricsService.RESULTS_LOG_FILENAME_SUFFIX), is(true));

        File resultsLog = new File(resultDir, configuration.name() + ThreadedQueuedConcurrentMetricsService.RESULTS_LOG_FILENAME_SUFFIX);
        CsvFileReader csvResultsLogReader = new CsvFileReader(resultsLog, CsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        assertThat((long) Iterators.size(csvResultsLogReader), is(configuration.operationCount() + 1)); // + 1 to account for csv headers
        csvResultsLogReader.closeReader();
    }
}
