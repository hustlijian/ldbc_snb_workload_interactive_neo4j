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

import com.ldbc.socialnet.neo4j.domain.Domain;
import com.ldbc.socialnet.neo4j.utils.Config;

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

    @Ignore
    @Test
    public void query1()
    {
        Map<String, Object> queryParams = Queries.LdbcInteractive.query1Params( "Chen" );
        execute( Queries.LdbcInteractive.QUERY_1_TEMPLATE, queryParams, 2, 10 );
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

        Map<String, Object> queryParams = Queries.LdbcInteractive.query2Params( personId, countryX, countryY,
                startDate, durationDays );
        // execute( Queries.LdbcInteractive.QUERY_3_TEMPLATE_x, queryParams, 0,
        // 1 );
        execute( Queries.LdbcInteractive.QUERY_3_TEMPLATE, queryParams, 1, 2 );
    }

    @Ignore
    @Test
    public void query4()
    {
        // TODO QUERY_4 is still incorrect, the RETURN is incorrect, it needs to
        // be fixed so tags are grouped by NAME first, and then counted

        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.NOVEMBER, 1 );

        long personId = 143;
        Date startDate = calendar.getTime();
        int durationDays = 10;

        Map<String, Object> queryParams = Queries.LdbcInteractive.query4Params( personId, startDate, durationDays );
        execute( Queries.LdbcInteractive.QUERY_4_TEMPLATE, queryParams, 0, 1 );
    }

    @Ignore
    @Test
    public void createIndexes()
    {
        queryEngine.execute( "DROP INDEX ON :" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")" );
        queryEngine.execute( "DROP INDEX ON :" + Domain.Node.PERSON + "(" + Domain.Person.FIRST_NAME + ")" );
        queryEngine.execute( "DROP INDEX ON :" + Domain.Node.PERSON + "(" + Domain.Person.LAST_NAME + ")" );
        queryEngine.execute( "DROP INDEX ON :" + Domain.Node.PLACE + "(" + Domain.Place.NAME + ")" );
        queryEngine.execute( "DROP INDEX ON :" + Domain.Place.Type.CITY + "(" + Domain.Place.NAME + ")" );
        queryEngine.execute( "DROP INDEX ON :" + Domain.Place.Type.COUNTRY + "(" + Domain.Place.NAME + ")" );

        queryEngine.execute( "CREATE INDEX ON :" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")" );
        queryEngine.execute( "CREATE INDEX ON :" + Domain.Node.PERSON + "(" + Domain.Person.FIRST_NAME + ")" );
        queryEngine.execute( "CREATE INDEX ON :" + Domain.Node.PERSON + "(" + Domain.Person.LAST_NAME + ")" );
        queryEngine.execute( "CREATE INDEX ON :" + Domain.Node.PLACE + "(" + Domain.Place.NAME + ")" );
        queryEngine.execute( "CREATE INDEX ON :" + Domain.Place.Type.CITY + "(" + Domain.Place.NAME + ")" );
        queryEngine.execute( "CREATE INDEX ON :" + Domain.Place.Type.COUNTRY + "(" + Domain.Place.NAME + ")" );
    }

    private void execute( String queryString, Map<String, Object> queryParams, long warmup, long iterations )
    {
        System.out.println( queryParams.toString() );
        System.out.println();
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
