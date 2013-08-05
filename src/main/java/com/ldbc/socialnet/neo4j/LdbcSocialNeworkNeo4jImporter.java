package com.ldbc.socialnet.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.neo4j.utils.Config;
import com.ldbc.socialnet.neo4j.utils.CsvFileInserters;

public class LdbcSocialNeworkNeo4jImporter
{
    /*
     TODO code improvements here
       - add properties file for pointing to resources
       - add readme with links to ldbc projects
       - is it necessary to store the "id" as a property if i want to index it?
          - is it good practice?
       - support reading series of csv lines and making them all available
          - for example, for when subsequent lines work on the same ID/Node/Rel
       - CsvFiles static field in util class
       - CsvInserters static field in util class
       - test that CsvFiles and CsvInserters contain same set of files
       - test that all CsvFiles are in specified folder, and that no others are in specified folder
       - add class to Domain with AttributeNames for each Entity
     TODO code improvements ldbc_driver
       - add toString for Time and Duration classes
       - use import java.util.concurrent.TimeUnit in my Time class
       - EMPTY_MAP in MapUtils?
    */

    private final static Logger logger = Logger.getLogger( LdbcSocialNeworkNeo4jImporter.class );

    public static void main( String[] args ) throws IOException
    {
        LdbcSocialNeworkNeo4jImporter ldbcSocialNetworkLoader = new LdbcSocialNeworkNeo4jImporter( Config.DB_DIR,
                Config.DATA_DIR );
        ldbcSocialNetworkLoader.load();
    }

    private final String dbDir;
    private final String csvDataDir;

    public LdbcSocialNeworkNeo4jImporter( String dbDir, String csvDataDir )
    {
        this.dbDir = dbDir;
        this.csvDataDir = csvDataDir;
    }

    public void load() throws IOException
    {
        logger.info( "Clear DB directory" );
        FileUtils.deleteRecursively( new File( dbDir ) );

        logger.info( "Instantiating Neo4j BatchInserter" );
        Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M" );
        BatchInserter batchInserter = BatchInserters.inserter( dbDir, config );

        BatchInserterIndexProvider batchIndexProvider = new LuceneBatchInserterIndexProvider( batchInserter );
        // BatchInserterIndexProvider batchIndexProvider = new
        // LuceneBatchInserterIndexProviderNewImpl( batchInserter );

        /*
        * CSV Files
        */
        List<CsvFileInserter> fileInserters = CsvFileInserters.all( batchInserter, batchIndexProvider, csvDataDir );

        logger.info( "Loading CSV files" );
        long startTime = System.currentTimeMillis();
        for ( CsvFileInserter fileInserter : fileInserters )
        {
            logger.info( String.format( "\t%s - %s", fileInserter.getFile().getName(), fileInserter.insertAllBuffered() ) );
        }
        long runtime = System.currentTimeMillis() - startTime;
        System.out.println( String.format(
                "Time: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                        - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        batchInserter.shutdown();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );

        logger.info( "Graph Metrics:" );
        logger.info( "\tNode count = " + nodeCount( db ) );
        logger.info( "\tRelationship count = " + relationshipCount( db ) );

        db.shutdown();
    }

    private static long nodeCount( GraphDatabaseService db )
    {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at( db );
        long nodeCount = -1;
        Transaction tx = db.beginTx();
        try
        {
            nodeCount = IteratorUtil.count( globalOperations.getAllNodes() );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getCause() );
        }
        finally
        {
            tx.finish();
        }
        return nodeCount;
    }

    private static long relationshipCount( GraphDatabaseService db )
    {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at( db );
        long relationshipCount = -1;
        Transaction tx = db.beginTx();
        try
        {
            relationshipCount = IteratorUtil.count( globalOperations.getAllRelationships() );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getCause() );
        }
        finally
        {
            tx.finish();
        }
        return relationshipCount;
    }
}
