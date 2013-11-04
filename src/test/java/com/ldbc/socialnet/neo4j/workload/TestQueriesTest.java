package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.util.FileUtils;

import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class TestQueriesTest
{
    public static String dbDir = "tempDb";
    public static GraphDatabaseService db = null;
    public static ExecutionEngine engine = null;

    @BeforeClass
    public static void openDb() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        engine = new ExecutionEngine( db );

        // TODO uncomment to print CREATE
        // System.out.println();
        // System.out.println( MapUtils.prettyPrint(
        // TestGraph.Creator.createGraphQueryParams() ) );
        // System.out.println( TestGraph.Creator.createGraphQuery() );

        buildGraph( engine, db );
        db.shutdown();
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        engine = new ExecutionEngine( db );
    }

    @AfterClass
    public static void closeDb() throws IOException
    {
        db.shutdown();
    }

    public static void buildGraph( ExecutionEngine engine, GraphDatabaseService db )
    {
        try (Transaction tx = db.beginTx())
        {
            engine.execute( TestGraph.Creator.createGraphQuery(), TestGraph.Creator.createGraphQueryParams() );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw e;
        }
        try (Transaction tx = db.beginTx())
        {
            for ( String createIndexQuery : TestGraph.Creator.createIndexQueries() )
            {
                engine.execute( createIndexQuery );
            }
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void personIdTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.PersonTestQuery.buildParams( 1, "alex", "averbuch" );
        assertThat( resultCount( TestQueries.PersonTestQuery.ID_QUERY_TEMPLATE, queryParams, "person" ), is( 1 ) );
        queryParams = TestQueries.PersonTestQuery.buildParams( 2, "aiya", "thorpe" );
        assertThat( resultCount( TestQueries.PersonTestQuery.ID_QUERY_TEMPLATE, queryParams, "person" ), is( 1 ) );
    }

    @Test
    public void personFirstNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.PersonTestQuery.buildParams( 1, "alex", "averbuch" );
        assertThat( resultCount( TestQueries.PersonTestQuery.FIRST_NAME_QUERY_TEMPLATE, queryParams, "person" ), is( 1 ) );
        queryParams = TestQueries.PersonTestQuery.buildParams( 2, "aiya", "thorpe" );
        assertThat( resultCount( TestQueries.PersonTestQuery.FIRST_NAME_QUERY_TEMPLATE, queryParams, "person" ), is( 1 ) );
    }

    @Test
    public void personLastNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.PersonTestQuery.buildParams( 1, "alex", "averbuch" );
        assertThat( resultCount( TestQueries.PersonTestQuery.LAST_NAME_QUERY_TEMPLATE, queryParams, "person" ), is( 1 ) );
        queryParams = TestQueries.PersonTestQuery.buildParams( 2, "aiya", "thorpe" );
        assertThat( resultCount( TestQueries.PersonTestQuery.LAST_NAME_QUERY_TEMPLATE, queryParams, "person" ), is( 1 ) );
    }

    @Test
    public void cityPlaceNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.CITY_PLACE_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
        queryParams = TestQueries.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.CITY_PLACE_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
    }

    @Test
    public void countryPlaceNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.COUNTRY_PLACE_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
        queryParams = TestQueries.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.COUNTRY_PLACE_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
    }

    @Test
    public void cityNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.CITY_NAME_QUERY_TEMPLATE, queryParams, "place" ), is( 1 ) );
        queryParams = TestQueries.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.CITY_NAME_QUERY_TEMPLATE, queryParams, "place" ), is( 1 ) );
    }

    @Test
    public void countryNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.COUNTRY_NAME_QUERY_TEMPLATE, queryParams, "place" ), is( 1 ) );
        queryParams = TestQueries.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat( resultCount( TestQueries.PlaceTestQuery.COUNTRY_NAME_QUERY_TEMPLATE, queryParams, "place" ), is( 1 ) );
    }

    int resultCount( String queryString, Map<String, Object> queryParams, String resultName )
    {
        try (Transaction tx = db.beginTx())
        {
            ExecutionResult resultWithIndex = engine.execute( queryString, queryParams );
            return IteratorUtil.count( resultWithIndex.columnAs( resultName ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return -1;
        }
    }

    String resultToString( Iterator<?> result )
    {
        StringBuilder sb = new StringBuilder();
        while ( result.hasNext() )
        {
            sb.append( result.next().toString() ).append( "\n" );
        }
        return sb.toString();
    }
}
