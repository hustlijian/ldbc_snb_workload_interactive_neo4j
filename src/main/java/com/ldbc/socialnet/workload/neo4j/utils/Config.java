package com.ldbc.socialnet.workload.neo4j.utils;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Config
{
    public static String get( String key )
    {
        return applicationConfig.getProperty( key );
    }

    private static Properties applicationConfig;
    public static Map<String, String> NEO4J_IMPORT_CONFIG;
    public static Map<String, String> NEO4J_RUN_CONFIG;

    static
    {
        try
        {
            applicationConfig = new Properties();
            applicationConfig.load( Config.class.getResourceAsStream( "/ldbc_neo4j.properties" ) );
            Map tempNeo4jImportConfig = new Properties();
            ( (Properties) tempNeo4jImportConfig ).load( Config.class.getResourceAsStream( get( "neo4j_import_config" ) ) );
            NEO4J_IMPORT_CONFIG = tempNeo4jImportConfig;
            Map tempNeo4jRunConfig = new Properties();
            ( (Properties) tempNeo4jRunConfig ).load( Config.class.getResourceAsStream( get( "neo4j_run_config" ) ) );
            NEO4J_RUN_CONFIG = tempNeo4jRunConfig;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        };
    }

    // CSV data
    public final static String DATA_DIR = get( "data_dir" );

    // Neo4j directory
    public final static String DB_DIR = get( "db_dir" );
}
