package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

@Ignore
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

    public enum TestLabels implements Label
    {
        TEST_LABEL_1,
        TEST_LABEL_2
    }

    @Test
    public void nodesShouldBeIndexed()
    {
        long nodeCount = 1000;
        System.out.println( "Loading " + nodeCount + " nodes..." );
        BatchInserter batchInserter = BatchInserters.inserter( "tempDb" );
        for ( int i = 0; i < nodeCount; i++ )
        {
            batchInserter.createNode( MapUtil.map( "id", i ), TestLabels.TEST_LABEL_1 );
        }
        System.out.println( "Nodes loaded" );
        System.out.println( "Create schema indexes" );
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL_1 ).on( "id" ).create();
        System.out.print( "Shut down batch inserter..." );
        batchInserter.shutdown();
        System.out.println( "Batch inserter shut down" );
        System.out.print( "Open GraphDatabaseFactory..." );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "tempDb" );
        System.out.println( "GraphDatabaseFactory open" );

        try (Transaction tx = db.beginTx())
        {
            for ( IndexDefinition indexDefinition : db.schema().getIndexes( TestLabels.TEST_LABEL_1 ) )
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
            db.shutdown();
            throw e;
        }

        ExecutionEngine queryEngine = new ExecutionEngine( db );

        String queryStringWithIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL_1 + ")\n"

        + "USING INDEX n:" + TestLabels.TEST_LABEL_1 + "(id)\n"

        + "WHERE n.id=42\n"

        + "RETURN n.id AS id";

        String queryStringWithoutIndex =

        "MATCH (n:" + TestLabels.TEST_LABEL_1 + ")\n"

        + "USING INDEX n:" + TestLabels.TEST_LABEL_1 + "(id)\n"

        + "WHERE n.id={id}\n"

        + "RETURN n.id AS id";

        try (Transaction tx = db.beginTx())
        {
            Map<String, Object> queryParams = MapUtil.map( "id", 42 );
            ExecutionResult resultWithIndex = execute( queryEngine, queryStringWithIndex, queryParams );
            ExecutionResult resultWithoutIndex = execute( queryEngine, queryStringWithoutIndex, queryParams );

            assertThat( IteratorUtil.count( resultWithIndex.columnAs( "id" ) ), is( 1 ) );
            assertThat( IteratorUtil.count( resultWithoutIndex.columnAs( "id" ) ), is( 1 ) );
        }
        catch ( Exception e )
        {
            throw e;
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void nodesShouldBeIndexedInMultipleIndexes()
    {
        long nodeCount = 1000;
        System.out.println( "Loading " + nodeCount + " nodes..." );
        BatchInserter batchInserter = BatchInserters.inserter( "tempDb" );
        for ( int i = 0; i < nodeCount; i++ )
        {
            batchInserter.createNode(
                    MapUtil.map( "id", i, "name", Integer.toString( i ), "id_name", String.format( "%s_%s", i, i ) ),
                    TestLabels.TEST_LABEL_1, TestLabels.TEST_LABEL_2 );
        }
        System.out.println( "Nodes loaded" );
        System.out.println( "Create schema indexes" );
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL_1 ).on( "id" ).create();
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL_1 ).on( "name" ).create();
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL_1 ).on( "id_name" ).create();
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL_2 ).on( "id" ).create();
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL_2 ).on( "name" ).create();
        batchInserter.createDeferredSchemaIndex( TestLabels.TEST_LABEL_2 ).on( "id_name" ).create();
        System.out.print( "Shut down batch inserter..." );
        batchInserter.shutdown();
        System.out.println( "Batch inserter shut down" );
        System.out.print( "Open GraphDatabaseFactory..." );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( "tempDb" );
        System.out.println( "GraphDatabaseFactory open" );
        try (Transaction tx = db.beginTx())
        {
            for ( IndexDefinition indexDefinition : db.schema().getIndexes() )
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
            db.shutdown();
            throw e;
        }

        ExecutionEngine queryEngine = new ExecutionEngine( db );

        try (Transaction tx = db.beginTx())
        {
            Map<String, Object> queryParams = MapUtil.map( "id", 42, "name", "42", "id_name", "42_42" );

            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "id", QueryVersion.WITH ), queryParams ).columnAs(
                            "id" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "id", QueryVersion.USING ), queryParams ).columnAs(
                            "id" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "id", QueryVersion.NONE ), queryParams ).columnAs(
                            "id" ) ), is( 1 ) );

            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "name", QueryVersion.WITH ), queryParams ).columnAs(
                            "name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "name", QueryVersion.USING ), queryParams ).columnAs(
                            "name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "name", QueryVersion.NONE ), queryParams ).columnAs(
                            "name" ) ), is( 1 ) );

            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "id_name", QueryVersion.WITH ), queryParams ).columnAs(
                            "id_name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "id_name", QueryVersion.USING ), queryParams ).columnAs(
                            "id_name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_1, "id_name", QueryVersion.NONE ), queryParams ).columnAs(
                            "id_name" ) ), is( 1 ) );

            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "id", QueryVersion.WITH ), queryParams ).columnAs(
                            "id" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "id", QueryVersion.USING ), queryParams ).columnAs(
                            "id" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "id", QueryVersion.NONE ), queryParams ).columnAs(
                            "id" ) ), is( 1 ) );

            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "name", QueryVersion.WITH ), queryParams ).columnAs(
                            "name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "name", QueryVersion.USING ), queryParams ).columnAs(
                            "name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "name", QueryVersion.NONE ), queryParams ).columnAs(
                            "name" ) ), is( 1 ) );

            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "id_name", QueryVersion.WITH ), queryParams ).columnAs(
                            "id_name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "id_name", QueryVersion.USING ), queryParams ).columnAs(
                            "id_name" ) ), is( 1 ) );
            assertThat(
                    IteratorUtil.count( execute( queryEngine,
                            buildQueryString( TestLabels.TEST_LABEL_2, "id_name", QueryVersion.NONE ), queryParams ).columnAs(
                            "id_name" ) ), is( 1 ) );
        }
        catch ( Exception e )
        {
            throw e;
        }
        finally
        {
            db.shutdown();
        }
    }

    enum QueryVersion
    {
        WITH,
        USING,
        NONE
    }

    private String buildQueryString( Label label, String property, QueryVersion queryVersion )
    {
        String s = null;
        switch ( queryVersion )
        {
        case WITH:
            s = "WITH n\n";
            break;
        case USING:
            s = "USING INDEX n:" + label + "(" + property + ")\n";
            break;

        case NONE:
            s = "";
            break;
        }
        return String.format(

        "MATCH (n:" + label + ")\n"

        + "%s"

        + "WHERE n." + property + "={" + property + "}\n"

        + "RETURN n." + property + " AS " + property + "",

        s );

    }

    private ExecutionResult execute( ExecutionEngine queryEngine, String queryString, Map<String, Object> queryParams )
    {
        System.out.println( queryString + "\n" );
        ExecutionResult result = queryEngine.execute( queryString, queryParams );
        return result;
    }

}
