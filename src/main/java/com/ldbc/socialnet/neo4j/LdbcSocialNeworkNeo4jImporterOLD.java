package com.ldbc.socialnet.neo4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.neo4j.domain.CommentsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.Domain;
import com.ldbc.socialnet.neo4j.domain.EmailAddressesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.ForumsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.LanguagesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.LocationsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.OrganisationsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.PersonsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.PostsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.TagClassesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.TagsBatchIndex;

/*
Buffer (100,000) & transform (relationship get) =   4 min, 50 sec
Buffer (100,000) & transform (identity) =           5 min, 17 sec
Buffer (100,000) =                                  5 min, 17 sec
No Buffer =                                         5 min, 29 sec
 */

/*
Files
  comment.csv - 1290084
  post.csv - 355341
  person.csv - 2000
  forum.csv - 25255
  tag.csv - 2098
  tagClass.csv - 57
  organisation.csv - 1721
  language.csv - 79
  location.csv - 1354
  emailaddress.csv - 3440
  comment_reply_of_comment.csv - 832156
  comment_reply_of_post.csv - 457928
  comment_located_location.csv - 1290084
  location_part_of_location.csv - 1348
  person_knows_person.csv - 59630
  person_study_at_organisation.csv - 1599
  person_speaks_language.csv - 4415
  person_creator_of_comment.csv - 1290084
  person_creator_of_post.csv - 355341
  person_moderator_of_forum.csv - 25255
  person_based_near_location.csv - 2000
  person_work_at_organisation.csv - 4385
  person_interest_tag.csv - 7091
  person_has_email_emailaddress.csv - 3440
  post_has_tag_tag.csv - 285855
  post_annotated_language.csv - 137471
  person_like_post.csv - 1347476
  post_located_location.csv - 355341
  forum_hasmember_person.csv - 537832
  forum_container_of_post.csv - 355341
  forum_hastag_tag.csv - 30346
  tag_has_type_tagclass.csv - 2098
  tagclass_is_subclass_of_tagclass.csv - 56
  organisation_based_near_location.csv - 1721
Graph Metrics:
  Node count = 1,681,430
  Relationship count = 7,355,787
 */

public class LdbcSocialNeworkNeo4jImporterOLD
{
    /*
     TODO code improvements here
       - add properties file for pointing to resources
       - add readme with links to ldbc projects
       - is it necessary to store the "id" as a property if i want to index it?
          - is it good practice?
     TODO code improvements ldbc_driver
       - add toString for Time and Duration classes
       - use import java.util.concurrent.TimeUnit in my Time class
     */

    /*
     TODO many relationships not documented in confluence csv section
     TODO schema/csv documentation inconsistent or incomplete in confluence
     TODO confluence relationship schema table should have start/end node types
     */

    private final static Logger logger = Logger.getLogger( LdbcSocialNeworkNeo4jImporterOLD.class );

    private final static String DB_DIR = "db";
    private final static String RAW_DATA_DIR = "/home/alex/workspace/java/ldbc_socialnet_bm_OLD/ldbc_socialnet_dbgen/outputDir/";
    private final static Map<String, Object> EMPTY_MAP = new HashMap<String, Object>();

    public static void main( String[] args ) throws IOException
    {
        LdbcSocialNeworkNeo4jImporterOLD ldbcSocialNetworkLoader = new LdbcSocialNeworkNeo4jImporterOLD( DB_DIR, RAW_DATA_DIR );
        ldbcSocialNetworkLoader.load();
    }

    private final List<CsvFileInserter> fileInserters;
    private final BatchInserter batchInserter;

    public LdbcSocialNeworkNeo4jImporterOLD( String dbDir, String csvDir ) throws IOException
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
        * Neo4j Batch Index Providers
        */
        CommentsBatchIndex commentsIndex = new CommentsBatchIndex( batchIndexProvider );
        PostsBatchIndex postsIndex = new PostsBatchIndex( batchIndexProvider );
        PersonsBatchIndex personsIndex = new PersonsBatchIndex( batchIndexProvider );
        ForumsBatchIndex forumsIndex = new ForumsBatchIndex( batchIndexProvider );
        TagsBatchIndex tagsIndex = new TagsBatchIndex( batchIndexProvider );
        TagClassesBatchIndex tagClassesIndex = new TagClassesBatchIndex( batchIndexProvider );
        OrganisationsBatchIndex organisationsIndex = new OrganisationsBatchIndex( batchIndexProvider );
        LanguagesBatchIndex languagesIndex = new LanguagesBatchIndex( batchIndexProvider );
        LocationsBatchIndex locationsIndex = new LocationsBatchIndex( batchIndexProvider );
        EmailAddressesBatchIndex emailAddressesIndex = new EmailAddressesBatchIndex( batchIndexProvider );

        /*
        * CSV Files
        */
        fileInserters = new ArrayList<CsvFileInserter>();
        fileInserters.add( comments( batchInserter, commentsIndex ) );
        fileInserters.add( posts( batchInserter, postsIndex ) );
        fileInserters.add( persons( batchInserter, personsIndex ) );
        fileInserters.add( forums( batchInserter, forumsIndex ) );
        fileInserters.add( tags( batchInserter, tagsIndex ) );
        fileInserters.add( tagClasses( batchInserter, tagClassesIndex ) );
        fileInserters.add( organisations( batchInserter, organisationsIndex ) );
        fileInserters.add( languages( batchInserter, languagesIndex ) );
        fileInserters.add( locations( batchInserter, locationsIndex ) );
        fileInserters.add( emailAddresses( batchInserter, emailAddressesIndex ) );
        fileInserters.add( commentsRepliedToComments( batchInserter, commentsIndex ) );
        fileInserters.add( commentsRepliedToPosts( batchInserter, commentsIndex, postsIndex ) );
        fileInserters.add( commentsLocatedInLocation( batchInserter, commentsIndex, locationsIndex ) );
        fileInserters.add( locationPartOfLocation( batchInserter, locationsIndex ) );
        fileInserters.add( personKnowsPerson( batchInserter, personsIndex ) );
        fileInserters.add( personStudiesAtOrganisation( batchInserter, personsIndex, organisationsIndex ) );
        fileInserters.add( personSpeaksLanguage( batchInserter, personsIndex, languagesIndex ) );
        fileInserters.add( personCreatorOfComment( batchInserter, personsIndex, commentsIndex ) );
        fileInserters.add( personCreatorOfPost( batchInserter, personsIndex, postsIndex ) );
        fileInserters.add( personModeratorOfForum( batchInserter, personsIndex, forumsIndex ) );
        fileInserters.add( personBasedNearLocation( batchInserter, personsIndex, locationsIndex ) );
        fileInserters.add( personWorksAtOrganisation( batchInserter, personsIndex, organisationsIndex ) );
        fileInserters.add( personHasInterestTag( batchInserter, personsIndex, tagsIndex ) );
        fileInserters.add( personHasEmailAddress( batchInserter, personsIndex, emailAddressesIndex ) );
        fileInserters.add( postHasTag( batchInserter, postsIndex, tagsIndex ) );
        fileInserters.add( postAnnotatedWithLanguage( batchInserter, postsIndex, languagesIndex ) );
        fileInserters.add( personLikesPost( batchInserter, personsIndex, postsIndex ) );
        fileInserters.add( postLocatedAtLocation( batchInserter, postsIndex, locationsIndex ) );
        fileInserters.add( forumHasMemberPerson( batchInserter, forumsIndex, personsIndex ) );
        fileInserters.add( forumContainerOfPost( batchInserter, forumsIndex, postsIndex ) );
        fileInserters.add( forumHasTag( batchInserter, forumsIndex, tagsIndex ) );
        fileInserters.add( tagHasTypeTagClass( batchInserter, tagsIndex, tagClassesIndex ) );
        fileInserters.add( tagClassHasSubclassOfTagClass( batchInserter, tagClassesIndex ) );
        fileInserters.add( organisationBasedNearLocation( batchInserter, organisationsIndex, locationsIndex ) );
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

    private static CsvFileInserter comments( final BatchInserter batchInserter, final CommentsBatchIndex commentsIndex )
            throws FileNotFoundException
    {
        /*
        id  creationDate            location IP     browser     content
        00  2010-03-11T10:11:18Z    14.134.0.11     Chrome      About Michael Jordan, Association...
         */

        return new CsvFileInserter( new File( RAW_DATA_DIR + "comment.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                // TODO convert to datetime
                // 2010-12-28T07:16:25Z
                properties.put( "creationDate", columnValues[1] );
                properties.put( "locationIP", columnValues[2] );
                properties.put( "browserUsed", columnValues[3] );
                properties.put( "content", columnValues[4] );
                long commentNodeId = batchInserter.createNode( properties, Domain.Node.COMMENT );
                commentsIndex.getIndex().add( commentNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter posts( final BatchInserter batchInserter, final PostsBatchIndex postsIndex )
            throws FileNotFoundException
    {
        /*
        TODO "retweet" relationship is in schema table but not in CSV files                        
        
        TODO csv file has varying number of columns - 6/7
        when 6: 
            [0]id           [1]???                                  [2]creationDate         [3]locationIP   [4]browserUsed  [5]content
            100             ???                                     2010-03-11T05:28:04Z    27.99.128.8     Firefox         About Michael Jordan
        when 7:             
            [0]id           [1]imageFile    [2]???                  [3]creationDate         [4]locationIP   [5]browserUsed  [6]content
            00              photo0.jpg      ???                     2011-01-15T07:01:20Z    143.106.0.7     Firefox         ???

        TODO "language" attribute appears in schema table but not in CSV-files and not in 
            
        TODO according to CSV-files: "id"   "photo"     "creationDate"  "ip"        "browser"       "content"
        TODO naming in schema:        id    imageFile   creationDate    locationIP  browserUsed     content
        */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "post.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                // TODO recheck below & uncomment all after generator is fixed
                // properties.put( "imageFile", columnValues[1] );
                // TODO dateTime
                // properties.put( "creationDate", columnValues[2] );
                // properties.put( "locationIP", columnValues[3] );
                // properties.put( "browserUsed", columnValues[4] );
                // properties.put( "content", columnValues[5] );
                long postNodeId = batchInserter.createNode( properties, Domain.Node.POST );
                postsIndex.getIndex().add( postNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter persons( final BatchInserter batchInserter, final PersonsBatchIndex personsIndex )
            throws FileNotFoundException
    {
        /*
        id      firstName   lastName    gender  birthday    creationDate            locationIP      browserUsed
        75      Fernanda    Alves       male    1984-12-15  2010-12-14T11:41:37Z    143.106.0.7     Firefox
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "firstName", columnValues[1] );
                properties.put( "lastName", columnValues[2] );
                properties.put( "gender", columnValues[3] );
                // TODO date
                properties.put( "birthday", columnValues[4] );
                // TODO datetime
                properties.put( "creationDate", columnValues[5] );
                properties.put( "locationIP", columnValues[6] );
                properties.put( "browserUsed", columnValues[7] );
                long personNodeId = batchInserter.createNode( properties, Domain.Node.PERSON );
                personsIndex.getIndex().add( personNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter forums( final BatchInserter batchInserter, final ForumsBatchIndex forumIndex )
            throws FileNotFoundException
    {
        /*
            // TODO "name" in CSV-files "title" in schema table
            id      title                       creationDate
            40220   Album 0 of Fernanda Alves   2011-01-15T07:01:19Z
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "forum.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "title", columnValues[1] );
                // TODO datetime
                properties.put( "creationDate", columnValues[2] );
                long forumNodeId = batchInserter.createNode( properties, Domain.Node.FORUM );
                forumIndex.getIndex().add( forumNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter tags( final BatchInserter batchInserter, final TagsBatchIndex tagIndex )
            throws FileNotFoundException
    {
        /*
        TODO naming: "tagClass" in schema table, "url" in CSV-files 
        TODO can we make this a relationship instead of an attribute? TagClass nodes exist already?
        TODO inconsistent with confluence. confluence does not mention hasType
        id      name                url/tagClass
        259     Gilberto_Gil        <http://dbpedia.org/resource/Gilberto_Gil>
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "tag.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                // TODO url or tagClass?
                properties.put( "url", columnValues[2] );
                long tagNodeId = batchInserter.createNode( properties, Domain.Node.TAG );
                tagIndex.getIndex().add( tagNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter tagClasses( final BatchInserter batchInserter,
            final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        TODO "url" attribute in CSV-files but not in schema
        TODO schema has relationship -subClassOf->TagClass

        id      name        url
        211     Person      http://dbpedia.org/ontology/Person
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "tagClass.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                // TODO can this be removed in favor of relationship only?
                properties.put( "url", columnValues[2] );
                long tagClassNodeId = batchInserter.createNode( properties, Domain.Node.TAG_CLASS );
                tagClassesIndex.getIndex().add( tagClassNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter organisations( final BatchInserter batchInserter,
            final OrganisationsBatchIndex organisationsIndex ) throws FileNotFoundException
    {
        /*
        TODO schema doc does not contain "url" attribute           
        id      name                            url
        00      Universidade_de_Pernambuco      <http://dbpedia.org/resource/Universidade_de_Pernambuco>
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "organisation.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                properties.put( "url", columnValues[2] );
                long organisationNodeId = batchInserter.createNode( properties, Domain.Node.ORGANISATION );
                organisationsIndex.getIndex().add( organisationNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter languages( final BatchInserter batchInserter, final LanguagesBatchIndex languageIndex )
            throws FileNotFoundException
    {
        /*
        TODO not documented in schema table

        id  name
        7   pt
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "language.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                long languageNodeId = batchInserter.createNode( properties, Domain.Node.LANGUAGE );
                languageIndex.getIndex().add( languageNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter locations( final BatchInserter batchInserter, final LocationsBatchIndex locationIndex )
            throws FileNotFoundException
    {
        /*
        TODO called Place in schema table and Location in CSV-files
        TODO "type" not mentioned in schema table
        id      name            url                                             type
        5170    South_America   <http://dbpedia.org/resource/South_America>     REGION
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "location.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                properties.put( "url", columnValues[2] );
                // LocationType = COUNTRY | CITY | REGION
                long locationNodeId = batchInserter.createNode( properties, Domain.Node.LOCATION,
                        Domain.LocationType.valueOf( (String) columnValues[3] ) );
                locationIndex.getIndex().add( locationNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter emailAddresses( final BatchInserter batchInserter,
            final EmailAddressesBatchIndex emailAddressIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition documented in confluence
        EmailAddress (documented)
        EmailAddress (in file - 2013/07/25)

        id      name
        00      Fernanda75@gmx.com
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "emailaddress.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                long emailAddressNodeId = batchInserter.createNode( properties, Domain.Node.EMAIL_ADDRESS );
                emailAddressIndex.getIndex().add( emailAddressNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter commentsRepliedToComments( final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex ) throws FileNotFoundException
    {
        /*
        TODO "repliedTo" not document in schema table
         
         id     (from)Comment.id    (to)Comment.id
         300    450                 420
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "comment_reply_of_comment.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromCommentNodeId = commentsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toCommentNodeId = commentsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromCommentNodeId, toCommentNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Domain.Rel.REPLY_OF,
                        properties );
            }
        } );
    }

    private static CsvFileInserter commentsRepliedToPosts( final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        TODO "repliedTo" not document in schema table
         
         id     (from)Comment.id    (to)Post.id
         300    450                 420
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "comment_reply_of_post.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromCommentNodeId = commentsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromCommentNodeId, toPostNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Domain.Rel.REPLY_OF,
                        properties );
            }
        } );
    }

    private static CsvFileInserter commentsLocatedInLocation( final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex, final LocationsBatchIndex locationsIndex )
            throws FileNotFoundException
    {
        /*
        TODO "locatedIn" not document in schema table
         
         id     (from)Comment.id    (to)Location.id
         00     100                 73
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "comment_located_location.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromCommentNodeId = commentsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toLocationNodeId = locationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromCommentNodeId, toLocationNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.IS_LOCATED_IN, properties );
            }
        } );
    }

    private static CsvFileInserter locationPartOfLocation( final BatchInserter batchInserter,
            final LocationsBatchIndex locationsIndex ) throws FileNotFoundException
    {
        /*
        TODO "partOf" not document in schema table
         
        id     (from)Location.id    (to)Location.id
        00      11                  5170
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "location_part_of_location.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromLocationNodeId = locationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toLocationNodeId = locationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromLocationNodeId, toLocationNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.IS_PART_OF, properties );
            }
        } );
    }

    private static CsvFileInserter personKnowsPerson( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex ) throws FileNotFoundException
    {
        /*
        TODO "knows" not document in schema table
         
        id      (from)Person.id     (to)Person.id
        00      11                  5170
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_knows_person.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromPersonNodeId, toPersonNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Domain.Rel.KNOWS,
                        properties );
            }
        } );
    }

    private static CsvFileInserter personStudiesAtOrganisation( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final OrganisationsBatchIndex organisationsIndex )
            throws FileNotFoundException
    {
        /*
        id          Person.id   Organization.id     classYear
        00          75          00                  2004
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_study_at_organisation.csv" ),
                new CsvLineInserter()
                {
                    @Override
                    public Object[] transform( Object[] columnValues )
                    {
                        int id = Integer.parseInt( (String) columnValues[0] );
                        long fromPersonNodeId = personsIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                        long toOrganisationNodeId = organisationsIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                        int classYear = Integer.parseInt( (String) columnValues[3] );
                        return new Object[] { id, fromPersonNodeId, toOrganisationNodeId, classYear };
                    }

                    @Override
                    public void insert( Object[] columnValues )
                    {
                        Map<String, Object> properties = new HashMap<String, Object>();
                        properties.put( "id", columnValues[0] );
                        properties.put( "classYear", columnValues[3] );
                        batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                                Domain.Rel.STUDY_AT, properties );
                    }
                } );
    }

    private static CsvFileInserter personSpeaksLanguage( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final LanguagesBatchIndex languagesIndex )
            throws FileNotFoundException
    {
        /*
        TODO "speaks" relationship not in schema table
        
        id  Person.id   Language.id
        00  75          7
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_speaks_language.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toLanguageNodeId = languagesIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromPersonNodeId, toLanguageNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Domain.Rel.SPEAKS,
                        properties );
            }
        } );
    }

    private static CsvFileInserter personCreatorOfComment( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final CommentsBatchIndex commentsIndex ) throws FileNotFoundException
    {
        /*
        TODO "creatorOf" relationship not in schema table
        
        id  Person.id   Comment.id
        00  75          7
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_creator_of_comment.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long toPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long fromCommentNodeId = commentsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, toPersonNodeId, fromCommentNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.HAS_CREATOR, properties );
            }
        } );
    }

    private static CsvFileInserter personCreatorOfPost( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        TODO "creatorOf" relationship not in schema table
        
        id  Person.id   Post.id
        00  75          7
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_creator_of_post.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long toPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long fromPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, toPersonNodeId, fromPostNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[2], (Long) columnValues[1],
                        Domain.Rel.HAS_CREATOR, properties );
            }
        } );
    }

    private static CsvFileInserter personModeratorOfForum( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final ForumsBatchIndex forumsIndex ) throws FileNotFoundException
    {
        /*
        TODO "hasModerator" relationship not in schema table
        
        id  Person.id   Forum.id
        00  75          7
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_moderator_of_forum.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long toPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long fromForumNodeId = 0;
                try
                {
                    fromForumNodeId = forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                }
                catch ( Exception e )
                {
                    /*
                     * TODO remove exception handling after data generator is fixed
                     * usually ids in colummn 0 of forum.csv (and other .csv files) have 0 suffix
                     * in forum.csv some rows do not, for example:
                     *    2978|Wall of Lei Liu|2010-03-11T03:55:32Z
                     * then files like person_moderator_of_forum.csv attempt to retrieve 29780
                     */
                    return null;
                }
                return new Object[] { id, toPersonNodeId, fromForumNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove after data generator fixed
                if ( columnValues == null ) return;

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[2], (Long) columnValues[1],
                        Domain.Rel.HAS_MODERATOR, properties );
            }
        } );
    }

    private static CsvFileInserter personBasedNearLocation( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final LocationsBatchIndex locationsIndex )
            throws FileNotFoundException
    {
        /*
        TODO "isLocatedIn" relationship not in schema table
        
        id  Person.id   Location.id
        00  75          7
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_based_near_location.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toLocationNodeId = locationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromPersonNodeId, toLocationNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.IS_LOCATED_IN, properties );
            }
        } );
    }

    private static CsvFileInserter personWorksAtOrganisation( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final OrganisationsBatchIndex organisationsIndex )
            throws FileNotFoundException
    {
        /*
        id  Person.id   Organization.id     workFrom
        00  75          10                  2016
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_work_at_organisation.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toOrganisationNodeId = organisationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                int workFrom = Integer.parseInt( (String) columnValues[3] );
                return new Object[] { id, fromPersonNodeId, toOrganisationNodeId, workFrom };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                properties.put( "workFrom", columnValues[3] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Domain.Rel.WORKS_AT,
                        properties );
            }
        } );
    }

    private static CsvFileInserter personHasInterestTag( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        TODO "hasInterest" relationship not in schema table

        id  Person.id   Tag.id
        00  75          259
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_interest_tag.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toTagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromPersonNodeId, toTagNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.HAS_INTEREST, properties );
            }
        } );
    }

    private static CsvFileInserter personHasEmailAddress( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final EmailAddressesBatchIndex emailAddressesIndex )
            throws FileNotFoundException
    {
        /*
        TODO "hasEmailAddress" relationship not in schema table

        id  Person.id   EmailAddress.id
        00  75          259
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_has_email_emailaddress.csv" ),
                new CsvLineInserter()
                {
                    @Override
                    public Object[] transform( Object[] columnValues )
                    {
                        int id = Integer.parseInt( (String) columnValues[0] );
                        long fromPersonNodeId = personsIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                        long toEmailAddressNodeId = emailAddressesIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                        return new Object[] { id, fromPersonNodeId, toEmailAddressNodeId };
                    }

                    @Override
                    public void insert( Object[] columnValues )
                    {
                        Map<String, Object> properties = new HashMap<String, Object>();
                        properties.put( "id", columnValues[0] );
                        batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                                Domain.Rel.HAS_EMAIL_ADDRESS, properties );
                    }
                } );
    }

    private static CsvFileInserter postHasTag( final BatchInserter batchInserter, final PostsBatchIndex postsIndex,
            final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        TODO "hasTag" relationship not in schema table

        id  Post.id     Tag.id
        00  75          259
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "post_has_tag_tag.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toTagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromPostNodeId, toTagNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Domain.Rel.HAS_TAG,
                        properties );
            }
        } );
    }

    private static CsvFileInserter postAnnotatedWithLanguage( final BatchInserter batchInserter,
            final PostsBatchIndex postsIndex, final LanguagesBatchIndex languagesIndex ) throws FileNotFoundException
    {
        /*
        TODO "annotatedWith" relationship not in schema table

        id  Post.id     Language.id
        00  75          259
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "post_annotated_language.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toLanguageNodeId = 0;
                try
                {
                    toLanguageNodeId = languagesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                }
                catch ( Exception e )
                {
                    /*
                     * TODO remove exception handling after data generator is fixed
                     * at present sometimes it occurs that languageId == -1
                     */
                    return null;
                }
                return new Object[] { id, fromPostNodeId, toLanguageNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove when data generator fixed
                if ( columnValues == null ) return;

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.ANNOTATED_WITH, properties );
            }
        } );
    }

    private static CsvFileInserter personLikesPost( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        id  Person.id   Post.id     creationDate
        00  1489        00          2011-01-20T11:18:41Z
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "person_like_post.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                // TODO dateTime
                Object creationDate = columnValues[3];
                return new Object[] { id, fromPersonNodeId, toPostNodeId, creationDate };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                properties.put( "creationDate", columnValues[3] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Domain.Rel.LIKES,
                        properties );
            }
        } );
    }

    private static CsvFileInserter postLocatedAtLocation( final BatchInserter batchInserter,
            final PostsBatchIndex postsIndex, final LocationsBatchIndex locationsIndex ) throws FileNotFoundException
    {
        /*
        TODO "locatedAt" relationship not in schema table

         id     Post.id     Location.Id
         00     100         73
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "post_located_location.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toLocationNodeId = locationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromPostNodeId, toLocationNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.IS_LOCATED_IN, properties );
            }
        } );
    }

    private static CsvFileInserter forumHasMemberPerson( final BatchInserter batchInserter,
            final ForumsBatchIndex forumsIndex, final PersonsBatchIndex personsIndex ) throws FileNotFoundException
    {
        /*
         TODO CSV-files documents only:     id     Forum.id    Person.id
         TODO in reality it's sometimes:    id     Forum.id    Person.id    joinDate
         TODO on purpose? if so document, if not fix
         
         id     Forum.id    Person.id   joinDate
         190    40240       1325        2010-11-10T03:40:43Z
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "forum_hasmember_person.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromForumNodeId = forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                long toPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();

                if ( columnValues.length == 4 )
                {
                    // TODO dateTime
                    Object joinDate = columnValues[3];
                    return new Object[] { id, fromForumNodeId, toPersonNodeId, joinDate };
                }
                else
                {
                    return new Object[] { id, fromForumNodeId, toPersonNodeId };
                }
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                if ( columnValues.length == 4 )
                {
                    properties.put( "joinDate", columnValues[3] );
                }
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.HAS_MEMBER, properties );
            }
        } );
    }

    private static CsvFileInserter forumContainerOfPost( final BatchInserter batchInserter,
            final ForumsBatchIndex forumsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        TODO "containerOf" relationship not documented in schema table
        
        id  Forum.id    Post.id
        00  40220       00
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "forum_container_of_post.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long fromForumNodeId = 0;
                try
                {
                    fromForumNodeId = forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                }
                catch ( Exception e )
                {
                    /*
                     * TODO remove exception handling after data generator is fixed
                     * usually ids in colummn 1 of forum_container_of_post.csv (and other .csv files) have 0 suffix
                     * in forum_container_of_post.csv some rows do not, for example:
                     *    50294
                     * then when trying to retrieve 50294 (probably supposed to be 502940) from forum.csv it is not found
                     */
                    return null;
                }
                long toPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                return new Object[] { id, fromForumNodeId, toPostNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove after data generator fixed
                if ( columnValues == null ) return;

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Domain.Rel.CONTAINER_OF, properties );
            }
        } );
    }

    private static CsvFileInserter forumHasTag( final BatchInserter batchInserter, final ForumsBatchIndex forumsIndex,
            final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        TODO "hasTag" relationship not documented in schema table

        TODO CSV-files says:    id  Forum.id    Tag.id
        TODO in reality it's:   id  Tag.id      Forum.id

        id      Tag.id  Forum.id
        303400  505200  1938
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "forum_hastag_tag.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                int id = Integer.parseInt( (String) columnValues[0] );
                long toTagNodeId = 0;
                try
                {
                    toTagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                }
                catch ( Exception e )
                {
                    /*
                     * TODO remove exception handling when generator fixed
                     * currently forum_hastag_tag.csv contains Tag.id entries in column 1 that are not in tag.csv
                     * for example: 75 in forum_hastag_tag.csv but the closest to that number in tag.csv is 74
                     */
                    return null;
                }
                long fromForumNodeId = 0;
                try
                {
                    fromForumNodeId = forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                }
                catch ( Exception e )
                {
                    /*
                     * TODO remove exception handling when generator is fixed
                     * almost ALL entries in forum_hastag_tag.csv column 2, Forum.id, contain id values that are not in forum.csv
                     * they have no 0 suffix either e.g. 6358
                     * 
                     * of 30346 entries only 1028 appear to be valid
                     */
                    return null;
                }
                return new Object[] { id, toTagNodeId, fromForumNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove after data generator fixed
                if ( columnValues == null ) return;

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[2], (Long) columnValues[1], Domain.Rel.HAS_TAG,
                        properties );
            }
        } );
    }

    private static CsvFileInserter tagHasTypeTagClass( final BatchInserter batchInserter,
            final TagsBatchIndex tagsIndex, final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        TODO "hasType" relationship not documented in schema table

        TODO CSV-files says:    id  Tag.id  TagClass.id
        TODO in reality it's:   Tag.id  TagClass.id

        Tag.id  TagClass.id
        259     211
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "tag_has_type_tagclass.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long fromTagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long toTagClassNodeId = tagClassesIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { fromTagNodeId, toTagClassNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.HAS_TYPE,
                        EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter tagClassHasSubclassOfTagClass( final BatchInserter batchInserter,
            final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        TODO "hasSubClass" relationship not documented in schema table

        TODO parent-sub order should be documented in CSV-files 
        
        (parent)TagClass.id  (sub)TagClass.id
        259                 211
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "tagclass_is_subclass_of_tagclass.csv" ),
                new CsvLineInserter()
                {
                    @Override
                    public Object[] transform( Object[] columnValues )
                    {
                        long parentTagClassNodeId = tagClassesIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                        long subTagClassNodeId = tagClassesIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                        return new Object[] { parentTagClassNodeId, subTagClassNodeId };
                    }

                    @Override
                    public void insert( Object[] columnValues )
                    {
                        batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                                Domain.Rel.IS_SUBCLASS_OF, EMPTY_MAP );
                    }
                } );
    }

    private static CsvFileInserter organisationBasedNearLocation( final BatchInserter batchInserter,
            final OrganisationsBatchIndex organisationsIndex, final LocationsBatchIndex locationsIndex )
            throws FileNotFoundException
    {
        /*
        TODO "isLocatedIn" relationship not documented in schema table

        TODO CSV-files specifies:   id  Person.id       Location.id
        TODO should be:             id  Organisation.id Location.id

        id      Organisation.id     Location.id
        17190   17190               3000
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "organisation_based_near_location.csv" ),
                new CsvLineInserter()
                {
                    @Override
                    public Object[] transform( Object[] columnValues )
                    {
                        int id = Integer.parseInt( (String) columnValues[0] );
                        long fromOrganisationNodeId = organisationsIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                        long toLocationNodeId = 0;
                        try
                        {
                            toLocationNodeId = locationsIndex.getIndex().get( "id",
                                    Integer.parseInt( (String) columnValues[2] ) ).getSingle();
                        }
                        catch ( Exception e )
                        {
                            /*
                             * TODO remove exception handling after generator fixed
                             * Location.id column contains ids that are not in location.csv
                             * eg. 301
                             */
                            return null;
                        }
                        return new Object[] { id, fromOrganisationNodeId, toLocationNodeId };
                    }

                    @Override
                    public void insert( Object[] columnValues )
                    {
                        // TODO remove when generator fixed
                        if ( columnValues == null ) return;

                        Map<String, Object> properties = new HashMap<String, Object>();
                        properties.put( "id", columnValues[0] );
                        batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                                Domain.Rel.IS_LOCATED_IN, properties );
                    }
                } );
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
