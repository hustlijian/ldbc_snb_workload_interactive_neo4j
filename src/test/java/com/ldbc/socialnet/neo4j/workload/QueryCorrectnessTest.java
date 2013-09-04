package com.ldbc.socialnet.neo4j.workload;

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

import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

// TODO build simple graph as part of test class rather than relying on real imported graph
@Ignore
public class QueryCorrectnessTest
{
    public static GraphDatabaseService db = null;
    public static ExecutionEngine queryEngine = null;

    @BeforeClass
    public static void openDb()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabase( Config.DB_DIR );
        queryEngine = new ExecutionEngine( db );
    }

    @AfterClass
    public static void closeDb()
    {
        db.shutdown();
    }

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
