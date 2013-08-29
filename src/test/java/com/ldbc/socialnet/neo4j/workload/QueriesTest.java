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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class QueriesTest
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
    public void query1()
    {
        Map<String, Object> queryParams = Queries.LdbcInteractive.Query1.buildParams( "Chen" );
        execute( Queries.LdbcInteractive.Query1.QUERY_TEMPLATE, queryParams, 2, 5, true );
    }

    @Test
    public void query3()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2010, Calendar.JANUARY, 1 );

        // long personId = 405;
        // String countryX = "India";
        // String countryY = "Pakistan";
        long personId = 143;
        String countryX = "United_States";
        String countryY = "Canada";
        Date startDate = calendar.getTime();
        int durationDays = 365 * 2;

        Map<String, Object> queryParams = Queries.LdbcInteractive.Query3.buildParams( personId, countryX, countryY,
                startDate, durationDays );
        execute( Queries.LdbcInteractive.Query3.QUERY_TEMPLATE, queryParams, 2, 5, true );
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
        execute( Queries.LdbcInteractive.Query4.QUERY_TEMPLATE, queryParams, 2, 5, true );
    }

    @Ignore
    @Test
    public void index()
    {
        tryToDropIndex( Domain.Node.PERSON, Domain.Person.ID );
        tryToDropIndex( Domain.Node.PERSON, Domain.Person.FIRST_NAME );
        tryToDropIndex( Domain.Node.PERSON, Domain.Person.LAST_NAME );
        tryToDropIndex( Domain.Node.PLACE, Domain.Place.NAME );
        tryToDropIndex( Domain.Place.Type.CITY, Domain.Place.NAME );
        tryToDropIndex( Domain.Place.Type.COUNTRY, Domain.Place.NAME );

        tryToCreateIndex( Domain.Node.PERSON, Domain.Person.ID );
        tryToCreateIndex( Domain.Node.PERSON, Domain.Person.FIRST_NAME );
        tryToCreateIndex( Domain.Node.PERSON, Domain.Person.LAST_NAME );
        tryToCreateIndex( Domain.Node.PLACE, Domain.Place.NAME );
        tryToCreateIndex( Domain.Place.Type.CITY, Domain.Place.NAME );
        tryToCreateIndex( Domain.Place.Type.COUNTRY, Domain.Place.NAME );
    }

    private void tryToDropIndex( Label label, String property )
    {
        try
        {
            String queryString = "DROP INDEX ON :" + label + "(" + property + ")";
            System.out.println( queryString );
            queryEngine.execute( queryString );
        }
        catch ( Exception e )
        {
            System.out.println( e.getMessage() );
        }
    }

    private void tryToCreateIndex( Label label, String property )
    {
        try
        {
            String queryString = "CREATE INDEX ON :" + label + "(" + property + ")";
            System.out.println( queryString );
            queryEngine.execute( queryString );
        }
        catch ( Exception e )
        {
            System.out.println( e.getMessage() );
        }
    }

    private void execute( String queryString, Map<String, Object> queryParams, long warmup, long iterations,
            boolean experimental )
    {
        System.out.println( queryParams.toString() );
        System.out.println();
        queryString = ( experimental ) ? "cypher experimental\n" + queryString : queryString;
        System.out.println( queryString );
        for ( int i = 0; i < warmup; i++ )
        {
            queryEngine.execute( queryString, queryParams );
            System.out.print( "." );
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
            System.out.print( "." );
        }
        System.out.println();
        System.out.println( "\truntimeTotal=" + runtimeTotal );
        System.out.println( "\truntimeRuns=" + runtimeRuns );
        System.out.println( "\truntimeMin=" + runtimeMin );
        System.out.println( "\truntimeMax=" + runtimeMax );
        System.out.println( result.dumpToString() );
    }
}
