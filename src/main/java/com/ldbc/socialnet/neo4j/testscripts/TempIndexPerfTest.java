package com.ldbc.socialnet.neo4j.testscripts;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.util.FileUtils;

import com.ldbc.socialnet.neo4j.tempindex.DirectMemoryMapDbTempIndexFactory;
import com.ldbc.socialnet.neo4j.tempindex.HashMapTempIndexFactory;
import com.ldbc.socialnet.neo4j.tempindex.MemoryMapDbTempIndexFactory;
import com.ldbc.socialnet.neo4j.tempindex.PersistentMapDbTempIndexFactory;
import com.ldbc.socialnet.neo4j.tempindex.TempIndex;
import com.ldbc.socialnet.neo4j.tempindex.TroveTempIndexFactory;

public class TempIndexPerfTest
{
    public static void main( String[] args ) throws IOException
    {
        long size = Long.parseLong( args[0] );

        File file = new File( "tempDir/" );
        file.mkdir();

        System.out.println( "Test size = " + size );

        List<TempIndex<Long, Long>> indexes = new ArrayList<TempIndex<Long, Long>>();
        indexes.add( new PersistentMapDbTempIndexFactory( file ).create() );
        indexes.add( new MemoryMapDbTempIndexFactory().create() );
        indexes.add( new DirectMemoryMapDbTempIndexFactory().create() );
        indexes.add( new TroveTempIndexFactory().create() );
        indexes.add( new HashMapTempIndexFactory().create() );

        for ( TempIndex<Long, Long> index : indexes )
        {
            doWritePerfTest( size, index );
            doReadPerfTest( size, index );
            index.shutdown();
            System.out.println();
        }

        FileUtils.deleteRecursively( file );
    }

    private static void doWritePerfTest( long size, TempIndex<Long, Long> index )
    {
        Random random = new Random( 42l );
        long startTime = System.currentTimeMillis();
        for ( long counter = 0; counter < size; counter++ )
        {
            index.put( counter, random.nextLong() );
        }
        long runtime = System.currentTimeMillis() - startTime;
        System.out.println( "Write["
                            + index.getClass().getSimpleName()
                            + "]: "
                            + String.format(
                                    "Time: %d min, %d sec",
                                    TimeUnit.MILLISECONDS.toMinutes( runtime ),
                                    TimeUnit.MILLISECONDS.toSeconds( runtime )
                                            - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );
    }

    private static void doReadPerfTest( long size, TempIndex<Long, Long> index )
    {
        long startTime = System.currentTimeMillis();
        for ( long counter = 0; counter < size; counter++ )
        {
            index.get( counter );
        }
        long runtime = System.currentTimeMillis() - startTime;
        System.out.println( "Read["
                            + index.getClass().getSimpleName()
                            + "]: "
                            + String.format(
                                    "Time: %d min, %d sec",
                                    TimeUnit.MILLISECONDS.toMinutes( runtime ),
                                    TimeUnit.MILLISECONDS.toSeconds( runtime )
                                            - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );
    }
}
