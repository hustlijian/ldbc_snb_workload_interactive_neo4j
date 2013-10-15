package com.ldbc.socialnet.neo4j.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.Operation;
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery3;
import com.ldbc.socialnet.workload.LdbcQuery4;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.LdbcQuery6;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery1EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery3EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery4EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery5EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery6EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@Ignore
public class QueryPerformanceTest
{
    public static final boolean PRINT = true;

    public static GraphDatabaseService db = null;
    public static ExecutionEngine engine = null;

    @BeforeClass
    public static void openDb()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( Config.DB_DIR ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        engine = new ExecutionEngine( db );
    }

    @AfterClass
    public static void closeDb()
    {
        db.shutdown();
    }

    @Ignore
    @Test
    public void names()
    {
        String query =

        "MATCH (person:PERSON)\n"

        + "RETURN DISTINCT person.firstName";

        ExecutionResult result = engine.execute( query );
        System.out.println( result.dumpToString() );
    }

    @Test
    public void query1()
    {
        Operation operation = new LdbcQuery1( "Joan", 10 );
        Neo4jQuery query = new Neo4jQuery1EmbeddedCypher();
        execute( "Query1", query, operation, 5, 5, false );
    }

    @Test
    public void query3()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );
        Date endDate = calendar.getTime();
        int durationDays = 365 * 1;

        long personId = 143;
        String countryX = "United_States";
        String countryY = "Canada";

        // personId = 405;
        // countryX = "India";
        // countryY = "Pakistan";

        Operation operation = new LdbcQuery3( personId, countryX, countryY, endDate, durationDays );
        Neo4jQuery query = new Neo4jQuery3EmbeddedCypher();
        execute( "Query3", query, operation, 5, 5, false );
    }

    @Test
    public void query4()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );

        long personId = 143;
        Date endDate = calendar.getTime();
        int durationDays = 300;

        Operation operation = new LdbcQuery4( personId, endDate, durationDays );
        Neo4jQuery query = new Neo4jQuery4EmbeddedCypher();
        execute( "Query4", query, operation, 5, 5, false );
    }

    @Test
    public void query5()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );

        long personId = 143;
        Date joinDate = calendar.getTime();

        Operation operation = new LdbcQuery5( personId, joinDate );
        Neo4jQuery query = new Neo4jQuery5EmbeddedCypher();
        execute( "Query5", query, operation, 5, 5, false );
    }

    @Test
    public void query6()
    {
        long personId = 143;

        String tagName = "Charles_Dickens";

        Operation operation = new LdbcQuery6( personId, tagName );
        Neo4jQuery query = new Neo4jQuery6EmbeddedCypher();
        execute( "Query6", query, operation, 5, 5, false );
    }

    private void execute( String name, Neo4jQuery query, Operation operation, long warmup, long iterations,
            boolean profile )
    {
        if ( PRINT )
        {
            System.out.println( name );
            System.out.println( query.description() );
        }
        for ( int i = 0; i < warmup; i++ )
        {
            Iterator<?> result = query.execute( db, engine, operation );
            while ( result.hasNext() )
            {
                result.next();
            }
            if ( PRINT ) System.out.print( "?" );
        }
        long runtimeTotal = 0;
        long runtimeRuns = 0;
        long runtimeMin = Long.MAX_VALUE;
        long runtimeMax = Long.MIN_VALUE;

        for ( int i = 0; i < iterations; i++ )
        {
            long start = System.currentTimeMillis();
            query.execute( db, engine, operation );
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
        }
        else
        {
            System.out.println( String.format( "%s: runtime mean=%s(ms)", name, runtimeTotal / runtimeRuns ) );
        }
    }

}
