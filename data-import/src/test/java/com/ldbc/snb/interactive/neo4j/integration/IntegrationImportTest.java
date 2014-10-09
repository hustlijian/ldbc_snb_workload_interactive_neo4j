package com.ldbc.snb.interactive.neo4j.integration;

import com.ldbc.driver.ClientException;
import com.ldbc.snb.interactive.neo4j.TestUtils;
import com.ldbc.snb.interactive.neo4j.load.LdbcSnbNeo4jImporter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.IOException;

public class IntegrationImportTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldLoadDatasetWithMain() throws ClientException, IOException {
        File dbDir = temporaryFolder.newFolder();
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter.main(new String[]{dbDir.getAbsolutePath(), csvFilesDir, configFilePath});
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir.getAbsolutePath());
        db.shutdown();
    }

    @Test
    public void shouldLoadDatasetUsingConstructor() throws ClientException, IOException {
        File dbDir = temporaryFolder.newFolder();
        String csvFilesDir = TestUtils.getResource("/test_csv_files").getAbsolutePath() + "/";
        String configFilePath = TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath();
        LdbcSnbNeo4jImporter ldbcSnbNeo4jImporter = new LdbcSnbNeo4jImporter();
        ldbcSnbNeo4jImporter.load(dbDir.getAbsolutePath(), csvFilesDir, configFilePath);
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir.getAbsolutePath());
        db.shutdown();
    }
}
