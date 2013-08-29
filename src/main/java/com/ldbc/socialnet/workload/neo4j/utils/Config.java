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
    public static Map<String, String> NEO4J_CONFIG;

    static
    {
        try
        {
            applicationConfig = new Properties();
            applicationConfig.load( Config.class.getResourceAsStream( "/neo4j_importer.properties" ) );
            Map tempNeo4jConfig = new Properties();
            ( (Properties) tempNeo4jConfig ).load( Config.class.getResourceAsStream( "/neo4j.properties" ) );
            NEO4J_CONFIG = tempNeo4jConfig;
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
