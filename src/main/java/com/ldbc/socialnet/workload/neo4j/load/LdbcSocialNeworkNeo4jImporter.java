package com.ldbc.socialnet.workload.neo4j.load;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
        BatchInserter batchInserter = BatchInserters.inserter( dbDir, Config.NEO4J_IMPORT_CONFIG );

        /*
        * CSV Files
        */
        TempIndexFactory<Long, Long> tempIndexFactory = new TroveTempIndexFactory();

        LdbcSocialNetworkCsvFileInserters fileInserters = new LdbcSocialNetworkCsvFileInserters( tempIndexFactory,
                batchInserter, csvDataDir );

        logger.info( "Loading CSV files" );
        long startTime = System.currentTimeMillis();

        // Node (Comment)
        insertFile( fileInserters.getCommentsInserter() );
        // Node (Person)
        insertFile( fileInserters.getPersonsInserter() );
        // Node (Place)
        insertFile( fileInserters.getPlacesInserter() );
        // Node (Post)
        insertFile( fileInserters.getPostsInserter() );

        // Relationship (Comment, Person)
        insertFile( fileInserters.getCommentHasCreatorPersonInserter() );
        // Relationship (Comment, Place)
        insertFile( fileInserters.getCommentIsLocatedInPlaceInserter() );
        // Relationship (Comment, Comment)
        insertFile( fileInserters.getCommentReplyOfCommentInserter() );
        // Relationship (Comment, Post)
        insertFile( fileInserters.getCommentReplyOfPostInserter() );

        // Free (Comment)
        logger.info( "Freeing comments index" );
        fileInserters.getCommentsIndex().shutdown();

        // Node (Forum)
        insertFile( fileInserters.getForumsInserter() );
        // Node (Tag)
        insertFile( fileInserters.getTagsInserter() );

        // Relationship (Forum, Post)
        insertFile( fileInserters.getForumContainerOfPostInserter() );
        // Relationship (Person, Post)
        insertFile( fileInserters.getPersonLikesPostInserter() );
        // Relationship (Post, Person)
        insertFile( fileInserters.getPostHasCreatorPersonInserter() );
        // Relationship (Post, Tag)
        insertFile( fileInserters.getPostHasTagTagInserter() );
        // Relationship (Post, Place)
        insertFile( fileInserters.getPostIsLocatedInPlaceInserter() );

        // Free (Post)
        logger.info( "Freeing posts index" );
        fileInserters.getPostsIndex().shutdown();

        // Relationship (Forum, Person)
        insertFile( fileInserters.getForumHasMemberPersonInserter() );
        // Relationship (Forum, Person)
        insertFile( fileInserters.getForumHasModeratorPersonInserter() );
        // Relationship (Forum, Tag)
        insertFile( fileInserters.getForumHasTagInserter() );

        // Free (Forum)
        logger.info( "Freeing forums index" );
        fileInserters.getForumsIndex().shutdown();

        // Node (TagClass)
        insertFile( fileInserters.getTagClassesInserter() );

        // Relationship (Tag, TagClass)
        insertFile( fileInserters.getTagClassIsSubclassOfTagClassInserter() );
        // Relationship (Tag, TagClass)
        insertFile( fileInserters.getTagHasTypeTagClassInserter() );

        // Free (TagClass)
        logger.info( "Freeing tag classes index" );
        fileInserters.getTagClassesIndex().shutdown();

        // Node (Organisation)
        insertFile( fileInserters.getOrganisationsInserter() );

        // Relationship (Person, Tag)
        insertFile( fileInserters.getPersonHasInterestTagInserter() );

        // Relationship (Person, Place)
        insertFile( fileInserters.getPersonIsLocatedInPlaceInserter() );
        // Relationship (Person, Person)
        insertFile( fileInserters.getPersonKnowsPersonInserter() );
        // Relationship (Place, Place)
        insertFile( fileInserters.getPlaceIsPartOfPlaceInserter() );

        // Node Property (Person)
        insertFile( fileInserters.getPersonHasEmailAddressInserter() );
        // Node Property (Person)
        insertFile( fileInserters.getPersonSpeaksLanguageInserter() );

        // Relationship (Person, Organisation)
        insertFile( fileInserters.getPersonStudyAtOrganisationInserter() );
        // Relationship (Person, Organisation)
        insertFile( fileInserters.getPersonWorksAtOrganisationInserter() );
        // Relationship (Organisation, Place)
        insertFile( fileInserters.getOrganisationBasedNearPlaceInserter() );

        // Free (Person)
        logger.info( "Freeing persons index" );
        fileInserters.getPersonsIndex().shutdown();
        // Free (Tag)
        logger.info( "Freeing tags index" );
        fileInserters.getTagsIndex().shutdown();
        // Free (Organisation)
        logger.info( "Freeing organisations index" );
        fileInserters.getOrganisationsIndex().shutdown();
        // Free (Place)
        logger.info( "Freeing places index" );
        fileInserters.getPlacesIndex().shutdown();

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

        // TODO move elsewhere?
        // logger.info( "Graph Metrics:" );
        // logger.info( "\tNode count = " + GraphUtils.nodeCount( db, 10000000 )
        // );
        // logger.info( "\tRelationship count = " +
        // GraphUtils.relationshipCount( db, 10000000 ) );

        db.shutdown();
    }

    private void insertFile( CsvFileInserter fileInserter ) throws FileNotFoundException
    {
        logger.info( String.format( "\t%s - %s", fileInserter.getFile().getName(), fileInserter.insertAllBuffered() ) );
    }
}
