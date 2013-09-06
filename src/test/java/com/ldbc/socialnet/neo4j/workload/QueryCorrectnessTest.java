package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.FileUtils;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.socialnet.workload.Queries;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@Ignore
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
        buildGraph( queryEngine );
    }

    @AfterClass
    public static void closeDb() throws IOException
    {
        db.shutdown();
        FileUtils.deleteRecursively( new File( dbDir ) );
    }

    private static void buildGraph( ExecutionEngine engine )
    {
        try (Transaction tx = db.beginTx())
        {
            System.out.println( MapUtils.prettyPrint( TestGraph.Creator.createGraphQueryParams() ) );
            System.out.println( TestGraph.Creator.createGraphQuery() );
            engine.execute( TestGraph.Creator.createGraphQuery(), TestGraph.Creator.createGraphQueryParams() );

            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
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

            // Map<String, Object> queryParams = MapUtil.map();
            // String queryString =
            // "MATCH (p:PERSON) WHERE p.firstName='alex' RETURN p";

            System.out.println( queryParams );
            System.out.println( queryString );

            ExecutionResult result = queryEngine.execute( queryString, queryParams );

            System.out.println( result.dumpToString() );

            // String firstName = (String) result.columnAs( "person." +
            // Domain.Person.FIRST_NAME ).next();
            //
            // assertThat( firstName, is( "alex" ) );

            /*
            person.%s, person.%s, person.%s, person.%s,\n"

                    + " person.%s, person.%s, person.%s, person.%s, person.%s,\n"

                    + " personCity.%s, uni.%s, studyAt.%s, uniCity.%s, company.%s, worksAt.%s,\n"

                    + " companyCountry.%s"             */

            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }

    @Ignore
    @Test
    public void personIdTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 75, "Fernanda", "Alves" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.ID_QUERY_TEMPLATE, queryParams, "person" ),
                is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 1159, "Lei", "Zhao" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.ID_QUERY_TEMPLATE, queryParams, "person" ),
                is( 1 ) );
    }

    @Ignore
    @Test
    public void personFirstNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 75, "Fernanda", "Alves" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.FIRST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 1159, "Lei", "Zhao" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.FIRST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 2 ) );
    }

    @Ignore
    @Test
    public void personLastNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 75, "Fernanda", "Alves" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.LAST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PersonTestQuery.buildParams( 1159, "Lei", "Zhao" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PersonTestQuery.LAST_NAME_QUERY_TEMPLATE, queryParams,
                        "person" ), is( 2 ) );
    }

    @Ignore
    @Test
    public void cityPlaceNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "São_Paulo", "Brazil" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "Veszprém", "Hungary" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
    }

    @Ignore
    @Test
    public void countryPlaceNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "São_Paulo", "Brazil" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.COUNTRY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "Veszprém", "Hungary" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.COUNTRY_PLACE_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
    }

    @Ignore
    @Test
    public void cityNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "São_Paulo", "Brazil" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "Veszprém", "Hungary" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.CITY_NAME_QUERY_TEMPLATE, queryParams, "place" ),
                is( 1 ) );
    }

    @Ignore
    @Test
    public void countryNameTest()
    {
        Map<String, Object> queryParams;
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "São_Paulo", "Brazil" );
        assertThat(
                resultCount( TestQueries.LdbcInteractive.PlaceTestQuery.COUNTRY_NAME_QUERY_TEMPLATE, queryParams,
                        "place" ), is( 1 ) );
        queryParams = TestQueries.LdbcInteractive.PlaceTestQuery.buildParams( "Veszprém", "Hungary" );
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
