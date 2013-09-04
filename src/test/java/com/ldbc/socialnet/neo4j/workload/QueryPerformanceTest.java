package com.ldbc.socialnet.neo4j.workload;

import java.util.Calendar;
import java.util.Date;
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
    public static final boolean PRINT = false;

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
    public void query1()
    {
        Map<String, Object> queryParams = Queries.LdbcInteractive.Query1.buildParams( "Chen" );
        execute( "Query1", Queries.LdbcInteractive.Query1.QUERY_TEMPLATE, queryParams, 2, 5, false );
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

        Map<String, Object> queryParams = Queries.LdbcInteractive.Query3.buildParams( personId, countryX, countryY,
                startDate, durationDays );
        execute( "Query3", Queries.LdbcInteractive.Query3.QUERY_TEMPLATE, queryParams, 2, 5, false );

        // personId = 405;
        // countryX = "India";
        // countryY = "Pakistan";
        //
        // queryParams = Queries.LdbcInteractive.Query3.buildParams( personId,
        // countryX, countryY, startDate, durationDays );
        // execute( Queries.LdbcInteractive.Query3.QUERY_TEMPLATE, queryParams,
        // 2, 10, true, false );
    }

    @Test
    public void query4()
    {
        // TODO QUERY_4 is still incorrect, the RETURN is incorrect, it needs to
        // be fixed so tags are grouped by NAME first, and then counted

        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );

        long personId = 143;
        Date startDate = calendar.getTime();
        int durationDays = 300;

        Map<String, Object> queryParams = Queries.LdbcInteractive.Query4.buildParams( personId, startDate, durationDays );
        execute( "Query4", Queries.LdbcInteractive.Query4.QUERY_TEMPLATE, queryParams, 2, 5, false );
        // execute( "Query4",
        // Queries.LdbcInteractive.Query4.QUERY_TEMPLATE_michael, queryParams,
        // 2, 5, false );
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
