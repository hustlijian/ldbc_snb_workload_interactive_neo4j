package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.workload.neo4j.utils.GraphUtils;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class SchemaIndexCreationTest
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
//        FileUtils.deleteRecursively( new File( dbDir ) );
    }

    public enum TestLabels implements Label
    {
        TEST_LABEL
    }

    @Test
    public void nodesShouldBeIndexed()
    {
        long nodeCount = 1000000;
        System.out.println( "Loading " + nodeCount + " nodes..." );
        BatchInserter batchInserter = BatchInserters.inserter( "tempDb" );
        for ( int i = 0; i < nodeCount; i++ )
        {
            batchInserter.createNode( MapUtil.map( "id", i ), TestLabels.TEST_LABEL );
        }
        System.out.println( "Nodes loaded" );
        System.out.println( "Create schema indexes" );
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL ).on( "id" ).create();
        System.out.print( "Shut down batch inserter..." );
        batchInserter.shutdown();
        System.out.println( "Batch inserter shut down" );
        System.out.print( "Open GraphDatabaseFactory..." );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "tempDb" );
        System.out.println( "GraphDatabaseFactory open" );
        Transaction tx = db.beginTx();
        try
        {
            for ( IndexDefinition indexDefinition : db.schema().getIndexes( TestLabels.TEST_LABEL ) )
            {
                IndexState indexState = db.schema().getIndexState( indexDefinition );
                System.out.println( String.format( "%s == %s", indexDefinition.toString(), indexState ) );
                assertThat( indexState, equalTo( IndexState.ONLINE ) );
            }

            System.out.println( GraphUtils.nodeCount( db, 100 ) );

            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
            throw e;
        }
        finally
        {
            tx.finish();
        }

        ExecutionEngine queryEngine = new ExecutionEngine( db );

        String queryStringWithIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL + ")\n"

        + "USING INDEX n:" + TestLabels.TEST_LABEL + "(id)\n"

        + "WHERE n.id=42\n"

        + "RETURN n";

        String queryStringWithoutIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL + ")\n"

        + "USING INDEX n:" + TestLabels.TEST_LABEL + "(id)\n"

        + "WHERE n.id={id}\n"

        + "RETURN n";

        Map<String, Object> queryParams = MapUtil.map( "id", 42 );
        ExecutionResult resultWithIndex = execute( queryEngine, queryStringWithIndex, queryParams );
        ExecutionResult resultWithoutIndex = execute( queryEngine, queryStringWithoutIndex, queryParams );

        assertThat( IteratorUtil.count( resultWithIndex.columnAs( "id" ) ), is( 1 ) );
        assertThat( IteratorUtil.count( resultWithoutIndex.columnAs( "id" ) ), is( 1 ) );

        db.shutdown();
    }

    @Test
    public void nodesShouldBeIndexedInMultipleIndexes()
    {
        long nodeCount = 1000000;
        System.out.println( "Loading " + nodeCount + " nodes..." );
        BatchInserter batchInserter = BatchInserters.inserter( "tempDb" );
        for ( int i = 0; i < nodeCount; i++ )
        {
            batchInserter.createNode(
                    MapUtil.map( "id", i, "name", Integer.toString( i ), "id_name", String.format( "%s_%s", i, i ) ),
                    TestLabels.TEST_LABEL );
        }
        System.out.println( "Nodes loaded" );
        System.out.println( "Create schema indexes" );
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL ).on( "id" ).create();
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL ).on( "name" ).create();
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL ).on( "id_name" ).create();
        System.out.print( "Shut down batch inserter..." );
        batchInserter.shutdown();
        System.out.println( "Batch inserter shut down" );
        System.out.print( "Open GraphDatabaseFactory..." );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "tempDb" );
        System.out.println( "GraphDatabaseFactory open" );
        Transaction tx = db.beginTx();
        try
        {
            for ( IndexDefinition indexDefinition : db.schema().getIndexes( TestLabels.TEST_LABEL ) )
            {
                IndexState indexState = db.schema().getIndexState( indexDefinition );
                System.out.println( String.format( "%s == %s", indexDefinition.toString(), indexState ) );
                assertThat( indexState, equalTo( IndexState.ONLINE ) );
            }

            System.out.println( GraphUtils.nodeCount( db, 100 ) );

            tx.success();
        }
        catch ( Exception e )
        {
            tx.failure();
            throw e;
        }
        finally
        {
            tx.finish();
        }

        ExecutionEngine queryEngine = new ExecutionEngine( db );

        String idQueryStringWithIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL + ")\n"

        + "USING INDEX n:" + TestLabels.TEST_LABEL + "(id)\n"

        + "WHERE n.id={id}\n"

        + "RETURN n.id AS id";

        String idQueryStringWithoutIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL + ")\n"

        + "WHERE n.id={id}\n"

        + "RETURN n.id AS id";

        String nameQueryStringWithIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL + ")\n"

        + "USING INDEX n:" + TestLabels.TEST_LABEL + "(name)\n"

        + "WHERE n.name={name}\n"

        + "RETURN n.name AS name";

        String nameQueryStringWithoutIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL + ")\n"

        + "WHERE n.name={name}\n"

        + "RETURN n.name AS name";

        Map<String, Object> queryParams = MapUtil.map( "id", 42, "name", "42", "id_name", "42_42" );

        assertThat( IteratorUtil.count( execute( queryEngine, idQueryStringWithIndex, queryParams ).columnAs( "id" ) ),
                is( 1 ) );
        assertThat(
                IteratorUtil.count( execute( queryEngine, idQueryStringWithoutIndex, queryParams ).columnAs( "id" ) ),
                is( 1 ) );
        assertThat(
                IteratorUtil.count( execute( queryEngine, nameQueryStringWithIndex, queryParams ).columnAs( "name" ) ),
                is( 1 ) );
        assertThat(
                IteratorUtil.count( execute( queryEngine, nameQueryStringWithoutIndex, queryParams ).columnAs( "name" ) ),
                is( 1 ) );

        db.shutdown();
    }

    private ExecutionResult execute( ExecutionEngine queryEngine, String queryString, Map<String, Object> queryParams )
    {
        queryString = "cypher experimental\n" + queryString;
        System.out.println( queryString );
        ExecutionResult result = queryEngine.execute( queryString, queryParams );
        // System.out.println( result.dumpToString() );
        return result;
    }

}
