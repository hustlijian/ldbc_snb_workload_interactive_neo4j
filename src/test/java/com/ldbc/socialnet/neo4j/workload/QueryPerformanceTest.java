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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.Operation;
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery3;
import com.ldbc.socialnet.workload.LdbcQuery4;
import com.ldbc.socialnet.workload.LdbcQuery5;
import com.ldbc.socialnet.workload.LdbcQuery6;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery3;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api.Neo4jQuery1EmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api.Neo4jQuery3EmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api.Neo4jQuery4EmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery1EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery3EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery4EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery5EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery6EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

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
    public void query1cypher()
    {
        try (Transaction tx = db.beginTx())
        {
            Operation operation = new LdbcQuery1( "Joan", 10 );
            Neo4jQuery query = new Neo4jQuery1EmbeddedCypher();
            execute( "Query1", query, operation, 2, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Test
    public void query1api()
    {
        try (Transaction tx = db.beginTx())
        {
            Operation operation = new LdbcQuery1( "Joan", 10 );
            Neo4jQuery query = new Neo4jQuery1EmbeddedApi();
            execute( "Query1", query, operation, 2, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Ignore
    @Test
    public void fofs()
    {
        String query =

        "MATCH (person:PERSON)-[:KNOWS*1..2]-(f:PERSON)\n"

        + "USING INDEX person:PERSON(id)\n"

        + "WHERE person.id=2 AND not(f=person)\n"

        + "RETURN DISTINCT id(f), f.firstName + ' ' + f.lastName AS friend\n";

        ExecutionResult result = engine.execute( query );
        System.out.println( result.dumpToString() );
    }

    @Ignore
    @Test
    public void fofscountryxposts()
    {
        String query =

        "MATCH (person:PERSON)-[:KNOWS*1..2]-(f:PERSON)\n"

                + "USING INDEX person:PERSON(id)\n"

                + "WHERE person.id=2 AND not(f=person)\n"

                + "WITH DISTINCT f AS friend\n"

                + "MATCH (friend)<-[:HAS_CREATOR]-(postX:POST)-[:IS_LOCATED_IN]->(countryX:COUNTRY)\n"

                + "USING INDEX countryX:COUNTRY(name)\n"

                + "WHERE countryX.name='United_States' AND postX.creationDate>=1262360339371 AND postX.creationDate<=1293896339371\n"

                + "RETURN id(friend), friend.firstName + ' ' + friend.lastName AS friend, count(DISTINCT postX) AS xCount\n"

                + "ORDER BY xCount DESC";

        ExecutionResult result = engine.execute( query );
        System.out.println( result.dumpToString() );
    }

    @Test
    public void query3cypher()
    {
        try (Transaction tx = db.beginTx())
        {
            Calendar calendar = Calendar.getInstance();
            calendar.set( 2011, Calendar.JANUARY, 1 );
            Date endDate = calendar.getTime();
            int durationDays = 365 * 1;

            long personId = 2;
            String countryX = "United_States";
            String countryY = "Canada";

            // personId = 405;
            // countryX = "India";
            // countryY = "Pakistan";

            LdbcQuery3 operation = new LdbcQuery3( personId, countryX, countryY, endDate, durationDays );
            Neo4jQuery3 query = new Neo4jQuery3EmbeddedCypher();
            execute( "Query3", query, operation, 2, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Test
    public void query3api()
    {
        try (Transaction tx = db.beginTx())
        {
            Calendar calendar = Calendar.getInstance();
            calendar.set( 2011, Calendar.JANUARY, 1 );
            Date endDate = calendar.getTime();
            int durationDays = 365 * 1;

            long personId = 2;
            String countryX = "United_States";
            String countryY = "Canada";

            // personId = 405;
            // countryX = "India";
            // countryY = "Pakistan";

            Operation operation = new LdbcQuery3( personId, countryX, countryY, endDate, durationDays );
            Neo4jQuery query = new Neo4jQuery3EmbeddedApi();
            execute( "Query3", query, operation, 2, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Test
    public void query4cypher()
    {
        try (Transaction tx = db.beginTx())
        {
            Calendar calendar = Calendar.getInstance();
            calendar.set( 2011, Calendar.JANUARY, 1 );

            long personId = 143;
            Date endDate = calendar.getTime();
            int durationDays = 300;

            Operation operation = new LdbcQuery4( personId, endDate, durationDays );
            Neo4jQuery query = new Neo4jQuery4EmbeddedCypher();
            execute( "Query4", query, operation, 2, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Test
    public void query4api()
    {
        try (Transaction tx = db.beginTx())
        {
            Calendar calendar = Calendar.getInstance();
            calendar.set( 2011, Calendar.JANUARY, 1 );

            long personId = 143;
            Date endDate = calendar.getTime();
            int durationDays = 300;

            Operation operation = new LdbcQuery4( personId, endDate, durationDays );
            Neo4jQuery query = new Neo4jQuery4EmbeddedApi();
            execute( "Query4", query, operation, 2, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Ignore
    @Test
    public void query5()
    {
        try (Transaction tx = db.beginTx())
        {
            Calendar calendar = Calendar.getInstance();
            calendar.set( 2011, Calendar.JANUARY, 1 );

            long personId = 143;
            Date joinDate = calendar.getTime();

            Operation operation = new LdbcQuery5( personId, joinDate );
            Neo4jQuery query = new Neo4jQuery5EmbeddedCypher();
            execute( "Query5", query, operation, 2, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    @Ignore
    @Test
    public void query6()
    {
        try (Transaction tx = db.beginTx())
        {
            long personId = 143;

            String tagName = "Charles_Dickens";

            Operation operation = new LdbcQuery6( personId, tagName );
            Neo4jQuery query = new Neo4jQuery6EmbeddedCypher();
            execute( "Query6", query, operation, 5, 5, false );
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
    }

    // TODO query7

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

        Iterator<?> result = null;

        for ( int i = 0; i < iterations; i++ )
        {
            long start = System.currentTimeMillis();
            result = query.execute( db, engine, operation );
            while ( ( i < iterations - 1 ) && result.hasNext() )
            {
                result.next();
            }
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
            while ( result.hasNext() )
            {
                System.out.println( result.next().toString() );
            }
        }
        else
        {
            System.out.println( String.format( "%s: runtime mean=%s(ms)", name, runtimeTotal / runtimeRuns ) );
        }
    }
}
