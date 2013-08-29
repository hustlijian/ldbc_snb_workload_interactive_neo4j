package com.ldbc.socialnet.neo4j.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

import com.ldbc.socialnet.load.neo4j.tempindex.DirectMemoryMapDbTempIndexFactory;
import com.ldbc.socialnet.load.neo4j.tempindex.HashMapTempIndexFactory;
import com.ldbc.socialnet.load.neo4j.tempindex.MemoryMapDbTempIndexFactory;
import com.ldbc.socialnet.load.neo4j.tempindex.PersistentMapDbTempIndexFactory;
import com.ldbc.socialnet.load.neo4j.tempindex.TempIndex;
import com.ldbc.socialnet.load.neo4j.tempindex.TroveTempIndexFactory;

@Ignore
public class PerfTests
{
    /*
    // -Xmx40g --> 421,000,000
    TempIndex<Long, Long> x = new TroveTempIndexFactory().create();
    */
    /*
    // -Xmx40g --> 81,000,000 ? strange behavior, very slow, buggy?
    TempIndex<Long, Long> x = new MemoryMapDbTempIndexFactory().create();
    */

    @Test
    public void shouldPerfTestTempIndexes() throws IOException
    {
        // Given
        String indexPath = "testIndex1/";
        FileUtils.deleteRecursively( new File( indexPath ) );
        File indexDir = new File( indexPath );
        indexDir.mkdir();

        // When
        List<TempIndex<Long, Long>> indexes = new ArrayList<TempIndex<Long, Long>>();
        indexes.add( new PersistentMapDbTempIndexFactory( indexDir ).create() );
        indexes.add( new MemoryMapDbTempIndexFactory().create() );
        indexes.add( new DirectMemoryMapDbTempIndexFactory().create() );
        indexes.add( new TroveTempIndexFactory().create() );
        indexes.add( new HashMapTempIndexFactory().create() );

        // Then
        for ( TempIndex<Long, Long> index : indexes )
        {
            doWritePerfTest( index );
            doReadPerfTest( index );
            index.shutdown();
            System.out.println();
        }

        FileUtils.deleteRecursively( new File( indexPath ) );
    }

    private void doWritePerfTest( TempIndex<Long, Long> index )
    {
        Random random = new Random( 42l );
        long startTime = System.currentTimeMillis();
        for ( long counter = 0; counter < 10000000; counter++ )
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

    private void doReadPerfTest( TempIndex<Long, Long> index )
    {
        long startTime = System.currentTimeMillis();
        for ( long counter = 0; counter < 10000000; counter++ )
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
