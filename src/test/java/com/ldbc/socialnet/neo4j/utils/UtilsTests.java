package com.ldbc.socialnet.neo4j.utils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.neo4j.CsvFileInserter;
import com.ldbc.socialnet.neo4j.tempindex.PersistentMapDbTempIndexFactory;
import com.ldbc.socialnet.neo4j.tempindex.TroveTempIndexFactory;

public class UtilsTests
{
    private final static String RAW_DATA_DIR = "/home/alex/workspace/java/ldbc_socialnet_bm/ldbc_socialnet_dbgen/outputDir/";

    @Test
    public void shouldBeNoDifferenceBetweenContentsOfGeneratedFilesFolderAndCsvFiles() throws IOException
    {
        // Given
        File csvFolder = new File( RAW_DATA_DIR );
        Set<String> filesInCsvFolder = new HashSet<String>();

        // When
        for ( File csvFile : csvFolder.listFiles() )
        {
            filesInCsvFolder.add( csvFile.getAbsolutePath() );
        }

        // Then
        assertThat( filesInCsvFolder, is( CsvFiles.all( Config.DATA_DIR ) ) );
    }

    @Test
    public void shouldBeNoDifferenceBetweenSetsOfCsvFilesAndFilesInCsvFileInserters() throws IOException
    {
        // Given
        String dbDirPath = "testDb1";
        FileUtils.deleteRecursively( new File( dbDirPath ) );
        BatchInserter batchInserter = BatchInserters.inserter( dbDirPath );

        String indexPath = "testIndex1/";
        FileUtils.deleteRecursively( new File( indexPath ) );
        File indexDir = new File( indexPath );
        indexDir.mkdir();

        // When
        Set<String> csvFileInserterFiles = new HashSet<String>( csvFileInsertersToFileNames( CsvFileInserters.all(
                new PersistentMapDbTempIndexFactory( indexDir ), batchInserter, Config.DATA_DIR ) ) );

        // Then
        assertThat( csvFileInserterFiles, is( CsvFiles.all( Config.DATA_DIR ) ) );

        FileUtils.deleteRecursively( new File( dbDirPath ) );
        FileUtils.deleteRecursively( new File( indexPath ) );
    }

    @Test
    public void shouldBeNoDuplicatesOfFilesInCsvFileInserters() throws IOException
    {
        // Given
        String dbDir = "testDb2";
        FileUtils.deleteRecursively( new File( dbDir ) );
        BatchInserter batchInserter = BatchInserters.inserter( dbDir );

        String indexPath = "testIndex1/";
        FileUtils.deleteRecursively( new File( indexPath ) );
        File indexDir = new File( indexPath );
        indexDir.mkdir();

        // When
        List<String> allCsvFileInsertersToFileNames = csvFileInsertersToFileNames( CsvFileInserters.all(
                new PersistentMapDbTempIndexFactory( indexDir ), batchInserter, Config.DATA_DIR ) );

        Set<String> noDuplcateCsvFileInserterFiles = new HashSet<String>( allCsvFileInsertersToFileNames );

        // Then
        assertThat( noDuplcateCsvFileInserterFiles.size(), is( allCsvFileInsertersToFileNames.size() ) );

        FileUtils.deleteRecursively( new File( dbDir ) );
        FileUtils.deleteRecursively( new File( indexPath ) );
    }

    private List<String> csvFileInsertersToFileNames( List<CsvFileInserter> csvFileInserters )
    {
        List<String> fileNames = new ArrayList<String>();
        for ( CsvFileInserter inserter : csvFileInserters )
        {
            fileNames.add( inserter.getFile().getAbsolutePath() );
        }
        return fileNames;
    }
}
