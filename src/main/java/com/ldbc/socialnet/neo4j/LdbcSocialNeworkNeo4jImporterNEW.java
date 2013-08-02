package com.ldbc.socialnet.neo4j;

import java.io.File;
import java.io.FileNotFoundException;
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

import com.ldbc.socialnet.neo4j.utils.CsvFileInserters;

/*
Buffer (100,000) & transform (relationship get) =   
Buffer (100,000) & transform (identity) =           
Buffer (100,000) =                                  
No Buffer =                                         
 */

/*
Files
Graph Metrics:
 */

public class LdbcSocialNeworkNeo4jImporterNEW
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

    /*
    TODO
    2) To remove the file "emailaddress.csv". Hence, the file "person_has_email_emailaddress.csv" will contain the corresponding email addresses.

    3) To remove the file "language.csv".
    Therefore:
    - The file "person_speaks_language.csv" will contain the corresponding language name (because "speaks" is a multi-valued attribute).
    - The file "post_annotated_language.csv" can be eliminated and the language of a post can be in the file "post.csv" (because "language" is an option and mono-valued attribute) .
    */

    private final static Logger logger = Logger.getLogger( LdbcSocialNeworkNeo4jImporterNEW.class );

    private final static String DB_DIR = "db";
    private final static String RAW_DATA_DIR = "/home/alex/workspace/java/ldbc_socialnet_bm/ldbc_socialnet_dbgen/outputDir/";
    private final static Map<String, Object> EMPTY_MAP = new HashMap<String, Object>();

    public static void main( String[] args ) throws IOException
    {
        LdbcSocialNeworkNeo4jImporterNEW ldbcSocialNetworkLoader = new LdbcSocialNeworkNeo4jImporterNEW( DB_DIR,
                RAW_DATA_DIR );
        ldbcSocialNetworkLoader.load();
    }

    private final List<CsvFileInserter> fileInserters;
    private final BatchInserter batchInserter;

    public LdbcSocialNeworkNeo4jImporterNEW( String dbDir, String csvDir ) throws IOException
    {
        logger.info( "Clear DB directory" );
        FileUtils.deleteRecursively( new File( dbDir ) );

        logger.info( "Instantiating Neo4j BatchInserter" );
        Map<String, String> config = new HashMap<String, String>();
        config.put( "neostore.nodestore.db.mapped_memory", "90M" );
        batchInserter = BatchInserters.inserter( DB_DIR, config );

        BatchInserterIndexProvider batchIndexProvider = new LuceneBatchInserterIndexProvider( batchInserter );
        // BatchInserterIndexProvider batchIndexProvider = new
        // LuceneBatchInserterIndexProviderNewImpl( batchInserter );

        /*
        * CSV Files
        */
        fileInserters = CsvFileInserters.all( batchInserter, batchIndexProvider );
    }

    public void load() throws FileNotFoundException
    {
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

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_DIR );

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
