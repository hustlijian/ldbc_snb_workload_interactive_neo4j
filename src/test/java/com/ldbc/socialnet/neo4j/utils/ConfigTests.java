package com.ldbc.socialnet.neo4j.utils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;


public class ConfigTests
{
    @Test
    public void shouldReturnCorrectValuesFromPropertiesFile()
    {
        // Given
        String expectedDataDir = "/home/alex/workspace/java/ldbc_socialnet_bm/ldbc_socialnet_dbgen/outputDir/";
        String expectedDbDir = "db";

        // When

        // Then
        assertThat( Config.DATA_DIR, is( expectedDataDir ) );
        assertThat( Config.DB_DIR, is( expectedDbDir ) );
    }
}
