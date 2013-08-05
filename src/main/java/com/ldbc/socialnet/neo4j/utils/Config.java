package com.ldbc.socialnet.neo4j.utils;

import java.io.IOException;
import java.util.Properties;

public class Config
{
    public static String get( String key )
    {
        return config.getProperty( key );
    }

    private static Properties config = new Properties();

    static
    {
        try
        {
            config.load( Config.class.getResourceAsStream( "/config.properties" ) );
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
