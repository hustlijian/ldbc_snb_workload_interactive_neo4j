package com.ldbc.socialnet.workload.neo4j.load;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.neo4j.load.tempindex.TempIndexFactory;
import com.ldbc.socialnet.workload.neo4j.load.tempindex.TroveTempIndexFactory;
import com.ldbc.socialnet.workload.neo4j.utils.Config;
import com.ldbc.socialnet.workload.neo4j.utils.GraphUtils;

public class LdbcSocialNeworkNeo4jImporter
{
    /*
     TODO code improvements here
       - make it possible/easier to free a TempIndex midway through load to free memory
       - add readme with links to ldbc projects
       - support reading series of csv lines and making them all available
          - for example, for when subsequent lines work on the same ID/Node/Rel
       - add class to Domain with AttributeNames for each Entity
       - make scaling.txt into a spreadsheet
       - add Message and MessageType.Comment|Post
     TODO code improvements ldbc_driver
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
        BatchInserter batchInserter = BatchInserters.inserter( dbDir, Config.NEO4J_CONFIG );

        /*
        * CSV Files
        */
        TempIndexFactory<Long, Long> tempIndexFactory = new TroveTempIndexFactory();
        List<CsvFileInserter> fileInserters = LdbcSocialNetworkCsvFileInserters.all( tempIndexFactory, batchInserter,
                csvDataDir );

        logger.info( "Loading CSV files" );
        long startTime = System.currentTimeMillis();
        for ( CsvFileInserter fileInserter : fileInserters )
        {
            logger.info( String.format( "\t%s - %s", fileInserter.getFile().getName(), fileInserter.insertAllBuffered() ) );
        }
        long runtime = System.currentTimeMillis() - startTime;
        System.out.println( String.format(
                "Data imported in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                        - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        logger.info( "Creating Indexes" );
        startTime = System.currentTimeMillis();

        GraphUtils.createDeferredSchemaIndexesUsingBatchInserter( batchInserter, Domain.labelPropertyPairsToIndex() );

        batchInserter.shutdown();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );

        GraphUtils.waitForIndexesToBeOnline( db, Domain.labelsToIndex() );

        runtime = System.currentTimeMillis() - startTime;
        System.out.println( String.format(
                "Indexes built in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes( runtime ),
                TimeUnit.MILLISECONDS.toSeconds( runtime )
                        - TimeUnit.MINUTES.toSeconds( TimeUnit.MILLISECONDS.toMinutes( runtime ) ) ) );

        logger.info( "Graph Metrics:" );
        logger.info( "\tNode count = " + GraphUtils.nodeCount( db, 10000000 ) );
        logger.info( "\tRelationship count = " + GraphUtils.relationshipCount( db, 10000000 ) );

        db.shutdown();
    }
}
