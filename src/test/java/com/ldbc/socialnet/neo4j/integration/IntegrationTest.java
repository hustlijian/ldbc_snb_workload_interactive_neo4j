package com.ldbc.socialnet.neo4j.integration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

import com.ldbc.driver.BenchmarkPhase;
import com.ldbc.driver.Client;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.WorkloadParams;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload;
import com.ldbc.socialnet.neo4j.workload.TestGraph;
import com.ldbc.socialnet.workload.neo4j.Neo4jDb;
import com.ldbc.socialnet.workload.neo4j.load.LdbcSocialNeworkNeo4jImporter;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class IntegrationTest
{
    public static String dbDir = "tempDb";

    @BeforeClass
    public static void openDb() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );
        ExecutionEngine queryEngine = new ExecutionEngine( db );

        buildGraph( db, queryEngine );
        db.shutdown();
    }

    @AfterClass
    public static void closeDb() throws IOException
    {
        FileUtils.deleteRecursively( new File( dbDir ) );
    }

    private static void buildGraph( GraphDatabaseService db, ExecutionEngine engine )
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
    public void shouldRunEmbeddedStepsTransactionalWorkloadWithoutThrowingException() throws ClientException
    {
        boolean exceptionThrown = false;
        try
        {
            long operationCount = 100;
            long recordCount = -1;
            int threadCount = 1;
            boolean showStatus = true;
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            Map<String, String> userParams = new HashMap<String, String>();
            userParams.put( LdbcInteractiveWorkload.PARAMETERS_FILENAME, "parameters.json" );
            userParams.put( Neo4jDb.PATH_KEY, dbDir );
            userParams.put( Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_STEPS );
            WorkloadParams params = new WorkloadParams( userParams, Neo4jDb.class.getName(),
                    LdbcInteractiveWorkload.class.getName(), operationCount, recordCount,
                    BenchmarkPhase.TRANSACTION_PHASE, threadCount, showStatus, timeUnit );

            Client client = new Client();
            client.start( params );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }

    @Test
    public void shouldRunEmbeddedCypherTransactionalWorkloadWithoutThrowingException() throws ClientException
    {
        boolean exceptionThrown = false;
        try
        {
            long operationCount = 100;
            long recordCount = -1;
            int threadCount = 1;
            boolean showStatus = true;
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            Map<String, String> userParams = new HashMap<String, String>();
            userParams.put( LdbcInteractiveWorkload.PARAMETERS_FILENAME, "parameters.json" );
            userParams.put( Neo4jDb.PATH_KEY, dbDir );
            userParams.put( Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE_EMBEDDED_CYPHER );
            WorkloadParams params = new WorkloadParams( userParams, Neo4jDb.class.getName(),
                    LdbcInteractiveWorkload.class.getName(), operationCount, recordCount,
                    BenchmarkPhase.TRANSACTION_PHASE, threadCount, showStatus, timeUnit );

            Client client = new Client();
            client.start( params );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }

    @Ignore
    // TODO should not depend on existence of a directory not created by test
    @Test
    public void shouldRunLoadWorkloadWithoutThrowingException() throws ClientException
    {
        boolean exceptionThrown = false;
        try
        {
            LdbcSocialNeworkNeo4jImporter.main( new String[] {} );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
    }
}
