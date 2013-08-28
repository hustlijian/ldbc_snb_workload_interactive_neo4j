package com.ldbc.socialnet.neo4j.utils.testscripts;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import com.ldbc.socialnet.neo4j.load.tempindex.DirectMemoryMapDbTempIndexFactory;
import com.ldbc.socialnet.neo4j.load.tempindex.HashMapTempIndexFactory;
import com.ldbc.socialnet.neo4j.load.tempindex.MemoryMapDbTempIndexFactory;
import com.ldbc.socialnet.neo4j.load.tempindex.PersistentMapDbTempIndexFactory;
import com.ldbc.socialnet.neo4j.load.tempindex.TempIndex;
import com.ldbc.socialnet.neo4j.load.tempindex.TroveTempIndexFactory;

public class TempIndexMaxCapacityTest
{
    public static void main( String[] args ) throws IOException
    {
        File file = new File( "tempDir/" );
        file.mkdir();
        file.deleteOnExit();
        TempIndex<Long, Long> index;
        // 14,000,000 -Xmx1g
        if ( args[0].equals( "memory_mapdb" ) )
            index = new MemoryMapDbTempIndexFactory().create();
        // 3,000,000 -XX:MaxDirectMemorySize=1g -Xmx128M
        // 31,000,000 -XX:MaxDirectMemorySize=1g -Xmx1g
        // 4,000,000 -XX:MaxDirectMemorySize=128m -Xmx1g
        else if ( args[0].equals( "direct_memory_mapdb" ) )
            index = new DirectMemoryMapDbTempIndexFactory().create();
        // 24,000,000+ -Xmx1g (very noticeably slower after 23,000,000)
        else if ( args[0].equals( "file_mapdb" ) )
            index = new PersistentMapDbTempIndexFactory( file ).create();
        // 13,000,000 -Xmx1g
        else if ( args[0].equals( "trove" ) )
            index = new TroveTempIndexFactory().create();
        // 9,000,000 -Xmx1g
        else if ( args[0].equals( "hashmap" ) )
            index = new HashMapTempIndexFactory().create();
        else
        {
            System.out.println( String.format( "Invalid TempIndex type - %s", args[0] ) );
            return;
        }

        System.out.println( index.getClass().getSimpleName() );
        Random random = new Random( 42l );
        for ( long counter = 0;; counter++ )
        {
            index.put( counter, random.nextLong() );
            if ( counter % 1000000 == 0 ) System.out.println( "" + counter );
        }
    }
}