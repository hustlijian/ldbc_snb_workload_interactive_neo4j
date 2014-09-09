package com.ldbc.socialnet.workload.neo4j.load;

import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.load.tempindex.TempIndexFactory;
import com.ldbc.socialnet.workload.neo4j.load.tempindex.TroveTempIndexFactory;
import com.ldbc.socialnet.workload.neo4j.utils.GraphUtils;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/*
MAVEN_OPTS="-server -XX:+UseConcMarkSweepGC -Xmx512m" mvn exec:java -Dexec.mainClass=com.ldbc.socialnet.workload.importer.LdbcSocialNetworkNeo4jImporter -Dexec.arguments="db/,/Users/alexaverbuch/IdeaProjects/ldbc_socialnet_bm/ldbc_socialnet_dbgen/outputDir/,/Users/alexaverbuch/IdeaProjects/ldbc_socialnet_importer/src/main/resources/neo4j_import_dev.properties"
 */

/*
TODO (Comment)-[IS_LOCATED]-(Place)
Comment.id|Place.id|

TODO (Comment)-[REPLY_OF]-(Comment)
Comment.id|Comment.id|

TODO (Comment)-[REPLY_OF]-(Post)
Comment.id|Post.id|

TODO (Forum)
id|title|creationDate|

TODO (Forum)-[CONTAINER_OF]-(Post)
Forum.id|Post.id|

TODO (Forum)-[HAS_MEMBER]-(Person)
Forum.id|Person.id|joinDate|

TODO (Forum)-[HAS_MODERATOR]-(Person)
Forum.id|Person.id|

TODO (Organization)
id|type|name|url|

TODO (Organization)-[LOCATED_IN]-(Place)
Organisation.id|Place.id|

TODO (Person)
id|firstName|lastName|gender|birthday|creationDate|locationIP|browserUsed|

TODO (Person).email
Person.id|email|

TODO (Person)-[HAS_INTEREST]-(Tag)
Person.id|Tag.id|

TODO (Person)-[LOCATED_IN]-(Place)
Person.id|Place.id|

TODO (Person)-[KNOWS]-(Person)
Person.id|Person.id|creationDate|

TODO (Person)-[LIKES]-(Comment)
Person.id|Comment.id|creationDate|

TODO (Person)-[LIKES]-(Post)
Person.id|Post.id|creationDate|

TODO (Person).language
Person.id|language|

TODO (Person)-[STUDY_AT]-(Organization)
Person.id|Organisation.id|classYear|

TODO (Person)-[WORK_AT]-(Organization)
Person.id|Organisation.id|workFrom|

TODO (Place)
id|name|url|type|

TODO (Place)-[IS_PART_OF]-(Place)
Place.id|Place.id|

TODO (Post)
id|imageFile|creationDate|locationIP|browserUsed|language|content|length|

TODO (Post)-[HAS_CREATOR]-(Person)
Post.id|Person.id|

TODO (Post)-[HAS_TAG]-(Tag)
Post.id|Tag.id|

TODO (Post)-[LOCATED_IN]-(Place)
Post.id|Place.id|

TODO (Tag)
id|name|url|

TODO (Tag)-[HAS_TYPE]-(TagClass)
Tag.id|TagClass.id|

TODO (TagClass)
id|name|url|

TODO (TagClass)-[IS_SUBCLASS_OF]-(TagClass)
TagClass.id|TagClass.id|
 */
public class LdbcSnbNeo4jImporter {
    private final static Logger logger = Logger.getLogger(LdbcSnbNeo4jImporter.class);

    public static void main(String[] args) throws IOException {
        if (args.length != 3)
            throw new RuntimeException(String.format("Expected 3 parameters: dbDir, dataDir, importerPropertiesPath\n" +
                    "Found %s: %s", args.length, Arrays.toString(args)));
        String dbDir = args[0];
        String dataDir = args[1];
        if (new File(dataDir).exists() == false)
            throw new RuntimeException(String.format("CSV not found: %s", dataDir));
        String importerPropertiesPath = args[2];
        if (new File(importerPropertiesPath).exists() == false)
            throw new RuntimeException(String.format("Neo4j properties file not found: %s", importerPropertiesPath));

        LdbcSnbNeo4jImporter ldbcSocialNetworkLoader = new LdbcSnbNeo4jImporter(dbDir, dataDir, importerPropertiesPath);
        ldbcSocialNetworkLoader.load();
    }

    private final String dbDir;
    private final String csvDataDir;
    private final Map<String, String> importerConfig;

    public LdbcSnbNeo4jImporter(String dbDirPath, String csvDataDir, String importerPropertiesPath) throws IOException {
        this.dbDir = new File(dbDirPath).getAbsolutePath();
        this.csvDataDir = csvDataDir;
        importerConfig = Utils.loadConfig(importerPropertiesPath);
    }

    public void load() throws IOException {
        logger.info(String.format("Clear DB directory: %s", dbDir));
        FileUtils.deleteRecursively(new File(dbDir));

        logger.info("Instantiating Neo4j BatchInserter");
        BatchInserter batchInserter = BatchInserters.inserter(dbDir, importerConfig);

        /*
        * CSV Files
        */
        TempIndexFactory<Long, Long> tempIndexFactory = new TroveTempIndexFactory();

        LdbcSocialNetworkCsvFileInserters fileInserters = new LdbcSocialNetworkCsvFileInserters(tempIndexFactory,
                batchInserter, csvDataDir);

        logger.info("Loading CSV files");
        long startTime = System.currentTimeMillis();

        // Node (Comment)
        insertFile(fileInserters.getCommentsInserter());
        // Node (Person)
        insertFile(fileInserters.getPersonsInserter());
        // Node (Place)
        insertFile(fileInserters.getPlacesInserter());
        // Node (Post)
        insertFile(fileInserters.getPostsInserter());

        // Relationship (Comment, Tag)
        insertFile(fileInserters.getCommentHasTagTagInserter());
        // Relationship (Comment, Person)
        insertFile(fileInserters.getCommentHasCreatorPersonInserter());
        // Relationship (Comment, Place)
        insertFile(fileInserters.getCommentIsLocatedInPlaceInserter());
        // Relationship (Comment, Comment)
        insertFile(fileInserters.getCommentReplyOfCommentInserter());
        // Relationship (Comment, Post)
        insertFile(fileInserters.getCommentReplyOfPostInserter());

        // Free (Comment)
        logger.info("Freeing comments index");
        fileInserters.getCommentsIndex().shutdown();

        // Node (Forum)
        insertFile(fileInserters.getForumsInserter());
        // Node (Tag)
        insertFile(fileInserters.getTagsInserter());

        // Relationship (Forum, Post)
        insertFile(fileInserters.getForumContainerOfPostInserter());
        // Relationship (Person, Post)
        insertFile(fileInserters.getPersonLikesPostInserter());
        // Relationship (Post, Person)
        insertFile(fileInserters.getPostHasCreatorPersonInserter());
        // Relationship (Post, Tag)
        insertFile(fileInserters.getPostHasTagTagInserter());
        // Relationship (Post, Place)
        insertFile(fileInserters.getPostIsLocatedInPlaceInserter());

        // Free (Post)
        logger.info("Freeing posts index");
        fileInserters.getPostsIndex().shutdown();

        // Relationship (Forum, Person)
        insertFile(fileInserters.getForumHasMemberPersonInserter());
        // Relationship (Forum, Person)
        insertFile(fileInserters.getForumHasModeratorPersonInserter());
        // Relationship (Forum, Tag)
        insertFile(fileInserters.getForumHasTagInserter());

        // Free (Forum)
        logger.info("Freeing forums index");
        fileInserters.getForumsIndex().shutdown();

        // Node (TagClass)
        insertFile(fileInserters.getTagClassesInserter());

        // Relationship (Tag, TagClass)
        insertFile(fileInserters.getTagClassIsSubclassOfTagClassInserter());
        // Relationship (Tag, TagClass)
        insertFile(fileInserters.getTagHasTypeTagClassInserter());

        // Free (TagClass)
        logger.info("Freeing tag classes index");
        fileInserters.getTagClassesIndex().shutdown();

        // Node (Organisation)
        insertFile(fileInserters.getOrganisationsInserter());

        // Relationship (Person, Tag)
        insertFile(fileInserters.getPersonHasInterestTagInserter());

        // Relationship (Person, Place)
        insertFile(fileInserters.getPersonIsLocatedInPlaceInserter());
        // Relationship (Person, Person)
        insertFile(fileInserters.getPersonKnowsPersonInserter());
        // Relationship (Place, Place)
        insertFile(fileInserters.getPlaceIsPartOfPlaceInserter());

        // Node Property (Person)
        insertFile(fileInserters.getPersonHasEmailAddressInserter());
        // Node Property (Person)
        insertFile(fileInserters.getPersonSpeaksLanguageInserter());

        // Relationship (Person, Organisation)
        insertFile(fileInserters.getPersonStudyAtOrganisationInserter());
        // Relationship (Person, Organisation)
        insertFile(fileInserters.getPersonWorksAtOrganisationInserter());
        // Relationship (Organisation, Place)
        insertFile(fileInserters.getOrganisationBasedNearPlaceInserter());

        // Free (Person)
        logger.info("Freeing persons index");
        fileInserters.getPersonsIndex().shutdown();
        // Free (Tag)
        logger.info("Freeing tags index");
        fileInserters.getTagsIndex().shutdown();
        // Free (Organisation)
        logger.info("Freeing organisations index");
        fileInserters.getOrganisationsIndex().shutdown();
        // Free (Place)
        logger.info("Freeing places index");
        fileInserters.getPlacesIndex().shutdown();

        long runtime = System.currentTimeMillis() - startTime;
        System.out.println(String.format(
                "Data imported in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(runtime),
                TimeUnit.MILLISECONDS.toSeconds(runtime)
                        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))));

        logger.info("Creating Indexes");
        startTime = System.currentTimeMillis();

        GraphUtils.createDeferredSchemaIndexesUsingBatchInserter(batchInserter, Domain.labelPropertyPairsToIndex());

        batchInserter.shutdown();

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(dbDir);

        GraphUtils.waitForIndexesToBeOnline(db, Domain.labelsToIndex());

        runtime = System.currentTimeMillis() - startTime;
        System.out.println(String.format(
                "Indexes built in: %d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(runtime),
                TimeUnit.MILLISECONDS.toSeconds(runtime)
                        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))));

        // TODO move elsewhere?
        // logger.info( "Graph Metrics:" );
        // logger.info( "\tNode count = " + GraphUtils.nodeCount( db, 10000000 )
        // );
        // logger.info( "\tRelationship count = " +
        // GraphUtils.relationshipCount( db, 10000000 ) );

        System.out.printf("Shutting down...");
        db.shutdown();
        System.out.println("Done");
    }

    private void insertFile(CsvFileInserter fileInserter) throws FileNotFoundException {
        logger.info(String.format("\t%s - %s", fileInserter.getFile().getName(), fileInserter.insertAllBuffered()));
    }
}
