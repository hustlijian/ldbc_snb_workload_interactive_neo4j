package com.ldbc.socialnet.neo4j.workload;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
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
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class QueryCorrectnessTest
{
    public static String dbDir = "tempDb";
    public static GraphDatabaseService db = null;
    public static ExecutionEngine queryEngine = null;

    /*

    I asked this yesterday but noone was online at that stage, so I'll try again...
    What's the recommended method for starting a Neo4j Server from within Java?
    I used to do:
    WrappingNeoServerBootstrapper server = new WrappingNeoServerBootstrapper( (GraphDatabaseAPI) embeddedDb );
    server.start()
    But it's now deprecated.
    IR suggested:
    CommunityNeoServer server = ServerBuilder.server()
                .withThirdPartyJaxRsPackage(
                        "org.neo4j.good_practices", "/colleagues" )
                .build();
    server.start();
    But then it turned out ServerBuilder is no longer in 2.0-SNAPSHOT
    Then I tried:
    GraphDatabaseService db = new GraphDatabaseFactory().
                                newEmbeddedDatabaseBuilder( path ).
                                setConfig( config ).
                                newGraphDatabase();
        Configurator configurator = new ServerConfigurator( (GraphDatabaseAPI) db );
        NeoServer server = new CommunityNeoServer( configurator );
    But I got a bunch of exceptions about "Unable to upgrade database".

    Any and all suggestions greatly appreciated!
     */

    @BeforeClass
    public static void openDb() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        queryEngine = new ExecutionEngine( db );

        // TODO uncomment to print CREATE
        System.out.println();
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
            Map<String, Object> queryParams = Queries.Query1.buildParams( "alex", 10 );
            String queryString = Queries.Query1.QUERY_TEMPLATE;

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

        Map<String, Object> queryParams = Queries.Query3.buildParams( personId, countryX, countryY, endDate,
                durationDays );
        String queryString = Queries.Query3.QUERY_TEMPLATE;

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
    public void query4ShouldReturnExpectedResult()
    {
        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.set( 2013, Calendar.SEPTEMBER, 8 );
        Date endDate = c.getTime();
        int durationDays = 3;

        Map<String, Object> queryParams = Queries.Query4.buildParams( personId, endDate, durationDays );
        String queryString = Queries.Query4.QUERY_TEMPLATE;

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            // TODO uncomment to print query
            // System.out.println( "\n" + queryParams + "\n\n" + queryString );

            ExecutionResult result = queryEngine.execute( queryString, queryParams );

            // TODO uncomment to print result
            // System.out.println( result.dumpToString() );

            int expectedRowCount = 5;
            int actualRowCount = 0;

            Map<String, Integer> validTags = new HashMap<String, Integer>();
            validTags.put( "pie", 3 );
            validTags.put( "lol", 2 );
            validTags.put( "cake", 1 );
            validTags.put( "yolo", 1 );
            validTags.put( "wtf", 1 );

            while ( result.iterator().hasNext() )
            {
                Map<String, Object> row = result.iterator().next();
                String tagName = (String) row.get( "tagName" );
                assertThat( validTags.containsKey( tagName ), is( true ) );
                int tagCount = (int) row.get( "tagCount" );
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
        Date date = c.getTime();

        Map<String, Object> queryParams = Queries.Query5.buildParams( personId, date );
        String queryStringPosts = Queries.Query5.QUERY_TEMPLATE_posts;
        String queryStringComments = Queries.Query5.QUERY_TEMPLATE_comments;

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            // TODO uncomment to print query
            // System.out.println( "\n" + queryParams + "\n\n" +
            // queryStringPosts );

            ExecutionResult resultPosts = queryEngine.execute( queryStringPosts, queryParams );

            // TODO uncomment to print result
            // System.out.println( resultPosts.dumpToString() );

            int expectedRowCount = 3;
            int actualRowCount = 0;

            Map<String, Long> validForumCounts = new HashMap<String, Long>();
            validForumCounts.put( "everything cakes and pies", 5L );
            validForumCounts.put( "boats are not submarines", 2L );
            validForumCounts.put( "kiwis sheep and bungy jumping", 1L );

            while ( resultPosts.iterator().hasNext() )
            {
                Map<String, Object> row = resultPosts.iterator().next();
                String tagName = (String) row.get( "forum" );
                assertThat( validForumCounts.containsKey( tagName ), is( true ) );
                long tagCount = (long) row.get( "posts" );
                assertThat( validForumCounts.get( tagName ), equalTo( tagCount ) );
                validForumCounts.remove( tagName );
                actualRowCount++;
            }
            assertThat( expectedRowCount, equalTo( actualRowCount ) );

            /*
             * 
             */

            // TODO uncomment to print query
            // System.out.println( "\n" + queryParams + "\n\n" +
            // queryStringComments );

            ExecutionResult resultComments = queryEngine.execute( queryStringComments, queryParams );

            // TODO uncomment to print result
            // System.out.println( resultComments.dumpToString() );

            expectedRowCount = 2;
            actualRowCount = 0;

            validForumCounts = new HashMap<String, Long>();
            validForumCounts.put( "everything cakes and pies", 2L );
            validForumCounts.put( "boats are not submarines", 1L );

            while ( resultComments.iterator().hasNext() )
            {
                Map<String, Object> row = resultComments.iterator().next();
                String forum = (String) row.get( "forum" );
                assertThat( validForumCounts.containsKey( forum ), is( true ) );
                long tagCount = (long) row.get( "comments" );
                assertThat( validForumCounts.get( forum ), equalTo( tagCount ) );
                validForumCounts.remove( forum );
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

        Map<String, Object> queryParams = Queries.Query6.buildParams( personId, tagName );
        String queryString = Queries.Query6.QUERY_TEMPLATE;

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            // TODO uncomment to print query
            // System.out.println( "\n" + queryParams + "\n\n" + queryString );

            ExecutionResult result = queryEngine.execute( queryString, queryParams );

            // TODO uncomment to print result
            // System.out.println( result.dumpToString() );

            int expectedRowCount = 3;
            int actualRowCount = 0;

            Map<String, Long> validTagCounts = new HashMap<String, Long>();
            validTagCounts.put( "wtf", 2L );
            validTagCounts.put( "pie", 2L );
            validTagCounts.put( "cake", 1L );

            while ( result.iterator().hasNext() )
            {
                Map<String, Object> row = result.iterator().next();
                String tag = (String) row.get( "tag" );
                assertThat( validTagCounts.containsKey( tag ), is( true ) );
                long tagCount = (long) row.get( "count" );
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

        Map<String, Object> queryParams = Queries.Query7.buildParams( personId, endDate, durationHours );
        String queryString = Queries.Query7.QUERY_TEMPLATE;

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx())
        {
            // TODO uncomment to print query
            // System.out.println( "\n" + queryParams + "\n\n" + queryString );

            ExecutionResult result = queryEngine.execute( queryString, queryParams );

            // TODO uncomment to print result
            // System.out.println( result.dumpToString() );

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
