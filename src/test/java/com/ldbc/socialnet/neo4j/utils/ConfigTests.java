package com.ldbc.socialnet.neo4j.utils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.junit.Test;

import com.ldbc.socialnet.workload.neo4j.utils.Config;


public class ConfigTests
{
    @Test
    public void shouldReturnCorrectValuesFromPropertiesFile()
    {
        // Given
        String expectedDbDir = "db";

        // When

        // Then
        assertThat( Config.DB_DIR, is( expectedDbDir ) );
    }
}
