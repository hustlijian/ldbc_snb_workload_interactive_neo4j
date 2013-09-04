package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
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

    @Test
    public void shouldUpdateStringArrayPropertiesOnNodesUsingBatchInserter()
    {
        BatchInserter batchInserter = BatchInserters.inserter( "tempDb" );

        String[] array1 = { "1" };
        long id1 = batchInserter.createNode( MapUtil.map( "array", array1 ) );
        long id2 = batchInserter.createNode( MapUtil.map( "array", array1 ) );

        String[] getId1Array1 = (String[]) batchInserter.getNodeProperties( id1 ).get( "array" );
        String[] getId2Array1 = (String[]) batchInserter.getNodeProperties( id2 ).get( "array" );
        assertThat( getId1Array1, equalTo( array1 ) );
        assertThat( getId2Array1, equalTo( array1 ) );
        String[] array12 = { "1", "2" };
        batchInserter.setNodeProperty( id1, "array", array12 );
        batchInserter.setNodeProperty( id2, "array", array12 );

        String[] getId1Array12 = (String[]) batchInserter.getNodeProperties( id1 ).get( "array" );
        String[] getId2Array12 = (String[]) batchInserter.getNodeProperties( id2 ).get( "array" );
        assertThat( getId1Array12, equalTo( array12 ) );
        assertThat( getId2Array12, equalTo( array12 ) );
        String[] array123 = { "1", "2", "3" };
        batchInserter.setNodeProperty( id1, "array", array123 );
        batchInserter.setNodeProperty( id2, "array", array123 );

        String[] getId1Array123 = (String[]) batchInserter.getNodeProperties( id1 ).get( "array" );
        String[] getId2Array123 = (String[]) batchInserter.getNodeProperties( id2 ).get( "array" );
        assertThat( getId1Array123, equalTo( array123 ) );
        assertThat( getId2Array123, equalTo( array123 ) );
        String[] array1234 = { "1", "2", "3", "4" };
        batchInserter.setNodeProperty( id1, "array", array1234 );
        batchInserter.setNodeProperty( id2, "array", array1234 );

        batchInserter.shutdown();
    }
}
