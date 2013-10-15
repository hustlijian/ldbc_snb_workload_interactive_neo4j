package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery1Result;
import com.ldbc.socialnet.workload.LdbcQuery3;
import com.ldbc.socialnet.workload.LdbcQuery3Result;
import com.ldbc.socialnet.workload.LdbcQuery4;
import com.ldbc.socialnet.workload.LdbcQuery4Result;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.LdbcQuery5Result;
import com.ldbc.socialnet.workload.LdbcQuery6;
import com.ldbc.socialnet.workload.LdbcQuery6Result;
import com.ldbc.socialnet.workload.LdbcQuery7;
import com.ldbc.socialnet.workload.LdbcQuery7Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery1;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery5;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery6;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery7;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery1EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery3EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery4EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery5EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery6EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery7EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class QueryCorrectnessTest
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
        System.out.println();
        System.out.println( MapUtils.prettyPrint( TestGraph.Creator.createGraphQueryParams() ) );
        System.out.println( TestGraph.Creator.createGraphQuery() );

        buildGraph( engine );
        db.shutdown();
        db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        engine = new ExecutionEngine( db );
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
        LdbcQuery1 operation1 = new LdbcQuery1( "alex", 10 );
        Neo4jQuery1 query1 = new Neo4jQuery1EmbeddedCypher();

        // TODO uncomment to print query
        System.out.println( "\n" + query1.description() + "\n\n" );

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            Iterator<LdbcQuery1Result> result = query1.execute( db, engine, operation1 );

            // Has 1 result
            assertThat( result.hasNext(), is( true ) );

            LdbcQuery1Result firstRow = result.next();

            // Has only 1 result
            assertThat( result.hasNext(), is( false ) );

            assertThat( firstRow.firstName(), is( "alex" ) );
            assertThat( firstRow.personCity(), is( "stockholm" ) );

            Set<String> resultCompanies = new HashSet<String>( firstRow.companies() );
            Set<String> expectedCompanies = new HashSet<String>( Arrays.asList( new String[] {
                    "swedish institute of computer science, sweden(2010)", "neo technology, sweden(2012)" } ) );
            assertThat( resultCompanies, equalTo( expectedCompanies ) );

            Set<String> resultUnis = new HashSet<String>( firstRow.unis() );
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

        LdbcQuery3 operation3 = new LdbcQuery3( personId, countryX, countryY, endDate, durationDays );
        Neo4jQuery3EmbeddedCypher query3 = new Neo4jQuery3EmbeddedCypher();

        // TODO uncomment to print query
        System.out.println( "\n" + query3.description() + "\n\n" );

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            Iterator<LdbcQuery3Result> result = query3.execute( db, engine, operation3 );

            // Has at least 1 result
            assertThat( result.hasNext(), is( true ) );

            LdbcQuery3Result firstRow = result.next();

            assertThat( firstRow.friendName(), is( "jacob hansson" ) );
            assertThat( firstRow.xCount(), is( 1L ) );
            assertThat( firstRow.yCount(), is( 2L ) );
            assertThat( firstRow.xyCount(), is( 3L ) );

            // Has at least 2 results
            assertThat( result.hasNext(), is( true ) );

            LdbcQuery3Result secondRow = result.next();

            assertThat( secondRow.friendName(), is( "aiya thorpe" ) );
            assertThat( secondRow.xCount(), is( 1L ) );
            assertThat( secondRow.yCount(), is( 1L ) );
            assertThat( secondRow.xyCount(), is( 2L ) );

            // Has exactly 2 results, no more
            assertThat( result.hasNext(), is( false ) );
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
    public void query4ShouldReturnExpectedResult()
    {
        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.set( 2013, Calendar.SEPTEMBER, 8 );
        Date endDate = c.getTime();
        int durationDays = 3;

        LdbcQuery4 operation4 = new LdbcQuery4( personId, endDate, durationDays );
        Neo4jQuery4EmbeddedCypher query4 = new Neo4jQuery4EmbeddedCypher();

        // TODO uncomment to print query
        System.out.println( "\n" + query4.description() + "\n\n" );

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            Iterator<LdbcQuery4Result> result = query4.execute( db, engine, operation4 );

            int expectedRowCount = 5;
            int actualRowCount = 0;

            Map<String, Integer> validTags = new HashMap<String, Integer>();
            validTags.put( "pie", 3 );
            validTags.put( "lol", 2 );
            validTags.put( "cake", 1 );
            validTags.put( "yolo", 1 );
            validTags.put( "wtf", 1 );

            while ( result.hasNext() )
            {
                LdbcQuery4Result row = result.next();
                String tagName = row.tagName();
                assertThat( validTags.containsKey( tagName ), is( true ) );
                int tagCount = row.tagCount();
                assertThat( validTags.get( tagName ), equalTo( tagCount ) );
                validTags.remove( tagName );
                actualRowCount++;
            }
            assertThat( expectedRowCount, equalTo( actualRowCount ) );

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
    public void query5ShouldReturnExpectedResult()
    {
        /*
        Friend
            Jake       Sweden
                5 September, New Zealand,  hello            [cake,yolo]     jakePost1           cakesAndPies                
                5 September, Sweden,       hej              [yolo]          jakePost2           cakesAndPies
                
                    Aiya        6 September     aiyaComment1        hi back
                    Stranger    7 September     strangerComment1    i don't know you
                        Aiya    7 September     aiyaComment2        so?
                    
                7 September, Sweden,       tjena            [wtf,lol,pie]   jakePost3       4   floatingBoats
            Peter      Germany
                7 September, Germany,      hallo            [pie,lol]       peterPost1      4   floatingBoats
                
                    Jake        7 September     jakeComment1        pity you couldn't come
                    
            Aiya       New Zealand
                6 September, Sweden,       kia ora          [pie,cake,yolo] aiyaPost1       4   cakesAndPies
                9 September, New Zealand,  bro              [lol]           aiyaPost2           cakesAndPies
                5 September, New Zealand,  chur             [cake, pie]     aiyaPost3           kiwisSheepAndBungyJumping
                
                    Alex        6 September     alexComment1        chur bro
        
        Not Friend             
            Stranger   Sweden
                2 September, Australia,    gidday           [pie, cake]     strangerPost1       cakesAndPies
                5 September, Australia,    I heart sheep    [lol]           strangerPost2       cakesAndPies

        JOINED
            cakesAndPies - 2013, Calendar.OCTOBER, 2
                Alex - 2013, Calendar.OCTOBER, 2 
                Aiya - 2013, Calendar.OCTOBER, 3
                Stranger - 2013, Calendar.OCTOBER, 4
                Jake - 2013, Calendar.OCTOBER, 8 

            redditAddicts - 2013, Calendar.OCTOBER, 22
                Jake - 2013, Calendar.OCTOBER, 22

            floatingBoats - 2013, Calendar.NOVEMBER, 13
                Jake - 2013, Calendar.NOVEMBER, 13 
                Alex - 2013, Calendar.NOVEMBER, 14
                Peter -  2013, Calendar.NOVEMBER, 16 

            kiwisSheepAndBungyJumping - 2013, Calendar.NOVEMBER, 1
                Aiya - 2013, Calendar.NOVEMBER, 1
                Alex - 2013, Calendar.NOVEMBER, 4

        FORUM                       POSTS           COMMENTS
        cakesAndPies                
                                    jakePost1       aiyaComment1
                                    jakePost2       strangerComment1(* not friend)
                                                    aiyaComment2
                                    aiyaPost1
                                    aiyaPost2
                                    strangerPost1(*)
                                    strangerPost2(*)
        floatingBoats
                                    jakePost3       jakeComment1
                                    peterPost1
        kiwisSheepAndBungyJumping
                                    aiyaPost3       alexComment1(* me, not friend)
        redditAddicts
        
        FORUM                       POSTS   COMMENTS
        cakesAndPies                4       2
        floatingBoats               2       1
        kiwisSheepAndBungyJumping   1       0
        redditAddicts               0       0

         */
        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.set( 2013, Calendar.JANUARY, 8 );
        Date joinDate = c.getTime();

        LdbcQuery5 operation5 = new LdbcQuery5( personId, joinDate );
        Neo4jQuery5 query5 = new Neo4jQuery5EmbeddedCypher();

        // TODO uncomment to print query
        System.out.println( "\n" + query5.description() + "\n\n" );

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            Iterator<LdbcQuery5Result> result = query5.execute( db, engine, operation5 );

            assertThat( result.hasNext(), is( true ) );
            assertThat( resultsEqual( result.next(), new LdbcQuery5Result( "everything cakes and pies", 5, 2 ) ),
                    is( true ) );
            assertThat( result.hasNext(), is( true ) );
            assertThat( resultsEqual( result.next(), new LdbcQuery5Result( "boats are not submarines", 2, 1 ) ),
                    is( true ) );
            assertThat( result.hasNext(), is( true ) );
            assertThat( resultsEqual( result.next(), new LdbcQuery5Result( "kiwis sheep and bungy jumping", 1, 0 ) ),
                    is( true ) );
            assertThat( result.hasNext(), is( false ) );

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
    public void query6ShouldReturnExpectedResult()
    {
        /*
        wtf 2
        pie 2
        cake 1

        Friend
            Jake       Sweden
                7 September, Sweden,       tjena            [wtf,lol,pie]   jakePost3       4   floatingBoats
            Peter      Germany
                7 September, Germany,      hallo            [pie,lol]       peterPost1      4   floatingBoats
            Aiya       New Zealand
                9 September, New Zealand,  bro              [lol]           aiyaPost2           cakesAndPies
            Nicky England
                5 September, England,    I live in england    [lol,cake,wtf]           nickyPost1       cakesAndPies
         */
        long personId = 1;
        String tagName = "lol";

        LdbcQuery6 operation6 = new LdbcQuery6( personId, tagName );
        Neo4jQuery6 query6 = new Neo4jQuery6EmbeddedCypher();

        // TODO uncomment to print query
        System.out.println( "\n" + query6.description() + "\n\n" );

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            Iterator<LdbcQuery6Result> result = query6.execute( db, engine, operation6 );

            int expectedRowCount = 3;
            int actualRowCount = 0;

            Map<String, Long> validTagCounts = new HashMap<String, Long>();
            validTagCounts.put( "wtf", 2L );
            validTagCounts.put( "pie", 2L );
            validTagCounts.put( "cake", 1L );

            while ( result.hasNext() )
            {
                LdbcQuery6Result row = result.next();
                String tag = row.tagName();
                assertThat( validTagCounts.containsKey( tag ), is( true ) );
                long tagCount = row.tagCount();
                assertThat( validTagCounts.get( tag ), equalTo( tagCount ) );
                validTagCounts.remove( tagName );
                actualRowCount++;
            }
            assertThat( expectedRowCount, equalTo( actualRowCount ) );

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
    public void query7ShouldReturnExpectedResult()
    {
        long personId = 1;

        Calendar c = Calendar.getInstance();
        c.set( 2013, Calendar.SEPTEMBER, 10 );
        Date endDate = c.getTime();
        int durationHours = 48;

        LdbcQuery7 operation7 = new LdbcQuery7( personId, endDate, durationHours );
        Neo4jQuery7 query7 = new Neo4jQuery7EmbeddedCypher();

        // TODO uncomment to print query
        System.out.println( "\n" + query7.description() + "\n\n" );

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            Iterator<LdbcQuery7Result> result = query7.execute( db, engine, operation7 );

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

    boolean resultsEqual( LdbcQuery5Result result1, LdbcQuery5Result result2 )
    {
        if ( false == result1.forum().equals( result2.forum() ) ) return false;
        if ( result1.postCount() != result2.postCount() ) return false;
        if ( result1.commentCount() != result2.commentCount() ) return false;
        if ( result1.count() != result2.count() ) return false;
        return true;
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

}
