package com.ldbc.socialnet.neo4j.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@Ignore
public class QueryPerformanceTest
{
    public static final boolean PRINT = true;

    public static GraphDatabaseService db = null;
    public static ExecutionEngine queryEngine = null;

    @BeforeClass
    public static void openDb()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( Config.DB_DIR ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        queryEngine = new ExecutionEngine( db );
    }

    @AfterClass
    public static void closeDb()
    {
        db.shutdown();
    }

    @Test
    public void query1()
    {
        Map<String, Object> queryParams = Queries.Query1.buildParams( "Chen", 10 );
        execute( "Query1", Queries.Query1.QUERY_TEMPLATE, queryParams, 5, 5, false );
    }

    @Test
    public void query3()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2010, Calendar.JANUARY, 1 );
        Date startDate = calendar.getTime();
        int durationDays = 365 * 1;

        long personId = 143;
        String countryX = "United_States";
        String countryY = "Canada";

        Map<String, Object> queryParams = Queries.Query3.buildParams( personId, countryX, countryY, startDate,
                durationDays );
        execute( "Query3", Queries.Query3.QUERY_TEMPLATE, queryParams, 5, 5, false );

        // personId = 405;
        // countryX = "India";
        // countryY = "Pakistan";
        //
        // queryParams = Queries.Query3.buildParams( personId,
        // countryX, countryY, startDate, durationDays );
        // execute( Queries.Query3.QUERY_TEMPLATE, queryParams,
        // 2, 10, true, false );
    }

    @Test
    public void query4()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );

        long personId = 143;
        Date startDate = calendar.getTime();
        int durationDays = 300;

        Map<String, Object> queryParams = Queries.Query4.buildParams( personId, startDate, durationDays );
        execute( "Query4", Queries.Query4.QUERY_TEMPLATE, queryParams, 5, 5, false );
    }

    @Test
    public void query5()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );

        long personId = 143;
        Date joinDate = calendar.getTime();

        Map<String, Object> queryParams = Queries.Query5.buildParams( personId, joinDate );
        execute( "Query5 - posts", Queries.Query5.QUERY_TEMPLATE_posts, queryParams, 5, 5, false );
        execute( "Query5 - comments", Queries.Query5.QUERY_TEMPLATE_comments, queryParams, 5, 5, false );
    }

    @Test
    public void query6()
    {
        long personId = 143;

        String tagName = "Charles_Dickens";

        Map<String, Object> queryParams = Queries.Query6.buildParams( personId, tagName );

        // queryParams = new HashMap<String, Object>();
        //
        // String prepQuery =
        //
        // "MATCH (person:PERSON)-[:KNOWS*1..2]-(:PERSON)<-[:HAS_CREATOR]-(:POST)-[:HAS_TAG]->(tag:TAG)\n"
        //
        // + "WHERE person.id=143\n"
        //
        // + "RETURN tag.name, count(tag) AS count\n"
        //
        // + "ORDER BY count DESC\n"
        //
        // + "LIMIT 10";

        /*
        +---------------------------------+
        | tag.name                | count |
        +---------------------------------+
        | "Charles_Dickens"       | 8036  |
        | "Heinrich_Himmler"      | 7524  |
        | "Herman_Melville"       | 7092  |
        | "Bottle_Pop"            | 6920  |
        | "Johann_Sebastian_Bach" | 6392  |
        | "Theodore_Roosevelt"    | 6140  |
        | "David_Gilmour"         | 5744  |
        | "Winston_Churchill"     | 5516  |
        | "Martin_Van_Buren"      | 5380  |
        | "Galileo_Galilei"       | 5368  |
        +---------------------------------+
         */

        // execute( "Query6 - prep", prepQuery, queryParams, 0, 1, false );
        execute( "Query6", Queries.Query6.QUERY_TEMPLATE, queryParams, 5, 5, false );
    }

    private void execute( String name, String queryString, Map<String, Object> queryParams, long warmup,
            long iterations, boolean profile )
    {
        queryString = ( profile ) ? "profile\n" + queryString : queryString;
        if ( PRINT )
        {
            System.out.println( queryParams.toString() );
            System.out.println();
            System.out.println( queryString );
        }
        for ( int i = 0; i < warmup; i++ )
        {
            queryEngine.execute( queryString, queryParams );
            if ( PRINT ) System.out.print( "?" );
        }
        long runtimeTotal = 0;
        long runtimeRuns = 0;
        long runtimeMin = Long.MAX_VALUE;
        long runtimeMax = Long.MIN_VALUE;
        ExecutionResult result = null;
        for ( int i = 0; i < iterations; i++ )
        {
            long start = System.currentTimeMillis();
            result = queryEngine.execute( queryString, queryParams );
            long runtime = System.currentTimeMillis() - start;
            runtimeTotal += runtime;
            runtimeRuns++;
            runtimeMin = Math.min( runtimeMin, runtime );
            runtimeMax = Math.max( runtimeMax, runtime );
            if ( PRINT ) System.out.print( "!" );
        }
        if ( PRINT )
        {
            System.out.println();
            System.out.println( "\truntimeTotal=" + runtimeTotal );
            System.out.println( "\truntimeRuns=" + runtimeRuns );
            System.out.println( "\truntimeMin=" + runtimeMin );
            System.out.println( "\truntimeMax=" + runtimeMax );
            System.out.println( "\truntimeMean=" + runtimeTotal / runtimeRuns );
            System.out.println( result.dumpToString() );
        }
        else
        {
            System.out.println( String.format( "%s: runtime mean=%s(ms)", name, runtimeTotal / runtimeRuns ) );
        }
    }
}
