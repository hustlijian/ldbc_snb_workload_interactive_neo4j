package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

import com.ldbc.driver.util.MapUtils;
import com.ldbc.socialnet.workload.Queries;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class QueryCorrectnessTest
{
    public static String dbDir = "tempDb";
    public static GraphDatabaseService db = null;
    public static ExecutionEngine queryEngine = null;

    @BeforeClass
    public static void openDb() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        queryEngine = new ExecutionEngine( db );

        // TODO remove
        System.out.println( MapUtils.prettyPrint( TestGraph.Creator.createGraphQueryParams() ) );
        System.out.println( TestGraph.Creator.createGraphQuery() );

        buildGraph( queryEngine );
        db.shutdown();
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        queryEngine = new ExecutionEngine( db );
    }

    @AfterClass
    public static void closeDb() throws IOException
    {
        db.shutdown();
    }

    private static void buildGraph( ExecutionEngine engine )
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
    public void query1ShouldReturnExpectedResult()
    {
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            Map<String, Object> queryParams = Queries.LdbcInteractive.Query1.buildParams( "alex", 10 );
            String queryString = Queries.LdbcInteractive.Query1.QUERY_TEMPLATE;

            // TODO uncomment to print query
            // System.out.println( "\n" + queryParams + "\n\n" + queryString );

            ExecutionResult result = queryEngine.execute( queryString, queryParams );

            // TODO uncomment to print result
            // System.out.println( result.dumpToString() );

            // Has 1 result
            assertThat( result.iterator().hasNext(), is( true ) );

            Map<String, Object> firstRow = result.iterator().next();

            // Has only 1 result
            assertThat( result.iterator().hasNext(), is( false ) );

            assertThat( (String) firstRow.get( "firstName" ), is( "alex" ) );
            assertThat( (String) firstRow.get( "personCity" ), is( "stockholm" ) );

            Set<String> resultCompanies = new HashSet<String>( (Collection<String>) firstRow.get( "companies" ) );
            Set<String> expectedCompanies = new HashSet<String>( Arrays.asList( new String[] {
                    "swedish institute of computer science, sweden(2010)", "neo technology, sweden(2012)" } ) );
            assertThat( resultCompanies, equalTo( expectedCompanies ) );

            Set<String> resultUnis = new HashSet<String>( (Collection<String>) firstRow.get( "unis" ) );
            Set<String> expectedUnis = new HashSet<String>( Arrays.asList( new String[] {
                    "royal institute of technology, stockholm(2008)",
                    "auckland university of technology, auckland(2006)" } ) );
            assertThat( resultUnis, equalTo( expectedUnis ) );

            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }

    @Test
    public void query3ShouldReturnExpectedResult()
    {
        long personId = 1;
        String countryX = "new zealand";
        String countryY = "sweden";
        Calendar c = Calendar.getInstance();
        c.set( 2013, Calendar.SEPTEMBER, 8 );
        Date endDate = c.getTime();
        int durationDays = 4;

        Map<String, Object> queryParams = Queries.LdbcInteractive.Query3.buildParams( personId, countryX, countryY,
                endDate, durationDays );
        String queryString = Queries.LdbcInteractive.Query3.QUERY_TEMPLATE;

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            // TODO uncomment to print query
            // System.out.println( "\n" + queryParams + "\n\n" + queryString );

            ExecutionResult result = queryEngine.execute( queryString, queryParams );

            // TODO uncomment to print result
            // System.out.println( result.dumpToString() );

            // Has at least 1 result
            assertThat( result.iterator().hasNext(), is( true ) );

            Map<String, Object> firstRow = result.iterator().next();

            assertThat( (String) firstRow.get( "friendName" ), is( "jacob hansson" ) );
            assertThat( (long) firstRow.get( "xCount" ), is( 1L ) );
            assertThat( (long) firstRow.get( "yCount" ), is( 2L ) );
            assertThat( (long) firstRow.get( "xyCount" ), is( 3L ) );

            // Has at least 2 results
            assertThat( result.iterator().hasNext(), is( true ) );

            Map<String, Object> secondRow = result.iterator().next();

            assertThat( (String) secondRow.get( "friendName" ), is( "aiya thorpe" ) );
            assertThat( (long) secondRow.get( "xCount" ), is( 1L ) );
            assertThat( (long) secondRow.get( "yCount" ), is( 1L ) );
            assertThat( (long) secondRow.get( "xyCount" ), is( 2L ) );

            // Has exactly 2 results, no more
            assertThat( result.iterator().hasNext(), is( false ) );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }

    @Test
    public void personIdTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 1, "alex", "averbuch" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.ID_QUERY_TEMPLATE, queryParams, "person" ),
                is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 2, "aiya", "thorpe" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.ID_QUERY_TEMPLATE, queryParams, "person" ),
                is( 1 ) );
    }

    @Test
    public void personFirstNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 1, "alex", "averbuch" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.FIRST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 2, "aiya", "thorpe" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.FIRST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 1 ) );
    }

    @Test
    public void personLastNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 1, "alex", "averbuch" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.LAST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 2, "aiya", "thorpe" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.LAST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 1 ) );
    }

    @Test
    public void cityPlaceNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
    }

    @Test
    public void countryPlaceNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.COUNTRY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.COUNTRY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
    }

    @Test
    public void cityNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
    }

    @Test
    public void countryNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "stockholm", "sweden" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.COUNTRY_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "auckland", "new zealand" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.COUNTRY_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
    }

    public int resultCount( String queryString, Map<String, Object> queryParams, String resultName )
    {
        try (Transaction tx = db.beginTx())
        {
            ExecutionResult resultWithIndex = queryEngine.execute( queryString, queryParams );
            return IteratorUtil.count( resultWithIndex.columnAs( resultName ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            return -1;
        }
    }

}
