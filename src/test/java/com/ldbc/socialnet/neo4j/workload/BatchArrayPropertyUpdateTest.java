package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class BatchArrayPropertyUpdateTest
{
    public static String dbDir = "tempDb";

    @Before
    public void deleteDirBefore() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
    }

    @After
    public void deleteDirAfter() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
    }

    // org.neo4j.kernel.impl.nioneo.store.InvalidRecordException:
    // DynamicRecord Not in use, blockId[1]
    @Test
    public void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter1()
    {
        // Given
        BatchInserter batchInserter = BatchInserters.inserter( dbDir );

        String[] array1 = { "1" };

        // When
        long id1 = batchInserter.createNode( MapUtil.map( "array", array1 ) );
        long id2 = batchInserter.createNode( MapUtil.map( "array", array1 ) );

        batchInserter.getNodeProperties( id1 ).get( "array" );
        batchInserter.setNodeProperty( id1, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array1 );

        batchInserter.getNodeProperties( id1 ).get( "array" );
        batchInserter.setNodeProperty( id1, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array1 );

        boolean invalidRecord = false;
        try
        {
            batchInserter.getNodeProperties( id1 ).get( "array" );
        }
        catch ( InvalidRecordException e )
        {
            System.out.println( e.getMessage() );
            invalidRecord = true;
        }
        finally
        {
            batchInserter.shutdown();
        }

        // Then
        assertThat( invalidRecord, is( true ) );
    }

    // org.neo4j.kernel.impl.nioneo.store.InvalidRecordException:
    // DynamicRecord Not in use, blockId[2]
    @Test
    public void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter2()
    {
        // Given
        BatchInserter batchInserter = BatchInserters.inserter( "tempDb" );

        String[] array1 = { "1" };

        // When
        long id1 = batchInserter.createNode( MapUtil.map( "array", array1 ) );
        long id2 = batchInserter.createNode( MapUtil.map( "array", array1 ) );

        batchInserter.getNodeProperties( id2 ).get( "array" );
        batchInserter.setNodeProperty( id2, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array1 );
        batchInserter.setNodeProperty( id2, "array", array1 );

        // NOTE, for some reason this needs to be, and needs to be id1
        batchInserter.getNodeProperties( id1 ).get( "array" );

        boolean invalidRecord = false;
        try
        {
            batchInserter.setNodeProperty( id2, "array", array1 );
        }
        catch ( InvalidRecordException e )
        {
            System.out.println( e.getMessage() );
            invalidRecord = true;
        }
        finally
        {
            batchInserter.shutdown();
        }

        // Then
        assertThat( invalidRecord, is( true ) );
    }
}
