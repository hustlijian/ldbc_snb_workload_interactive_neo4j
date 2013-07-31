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
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProviderNewImpl;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.ldbc.socialnet.neo4j.domain.CommentsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.EmailAddressesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.ForumsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.LanguagesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.LocationsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.Nodes;
import com.ldbc.socialnet.neo4j.domain.OrganisationsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.PersonsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.PostsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.Relationships;
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

public class LdbcSocialNeworkNeo4jImporter
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

    private final static Logger logger = Logger.getLogger( LdbcSocialNeworkNeo4jImporter.class );

    private final static String DB_DIR = "db";
    private final static String RAW_DATA_DIR = "/home/alex/workspace/java/ldbc_socialnet_bm/ldbc_socialnet_dbgen/outputDir/";
    private final static Map<String, Object> EMPTY_MAP = new HashMap<String, Object>();

    public static void main( String[] args ) throws IOException
    {
        LdbcSocialNeworkNeo4jImporter ldbcSocialNetworkLoader = new LdbcSocialNeworkNeo4jImporter( DB_DIR, RAW_DATA_DIR );
        ldbcSocialNetworkLoader.load();
    }

    private final List<CsvFileInserter> fileInserters;
    private final BatchInserter batchInserter;

    public LdbcSocialNeworkNeo4jImporter( String dbDir, String csvDir ) throws IOException
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
        Comment (documented)
            id              int         unique              1   1   sequential
            creationDate    dateTime    attribute           1   1
            hasCreator      Person      directed relation   1   1
            content         string      attribute           1   1
            browserUsed     string      attribute           0   1   Chrone, IE, Firefox
            locationIP      string      attribute           1   1
            isLocatedIn     Country     directed relation   0   1   Person.IsLocatedIn
            replyOf         Post        directed relation   0   1
            replyOf         Comment     directed relation   0   1
        Comment (in file - 2013/07/25)
            id              int         unique              1   1   sequential
            creationDate    dateTime    attribute           1   1
            locationIP      string      attribute           1   1
            browserUsed     string      attribute           0   1   Chrone, IE, Firefox
            content         string      attribute           1   1
        
        // TODO not documented in confluence
        id  date                    location IP     browser     content
        00  2010-03-11T10:11:18Z    14.134.0.11     Chrome      About Michael Jordan, Association (NBA) website states, By acclamation, Michael Jordan is the greatest basketball player of all.
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
                long commentNodeId = batchInserter.createNode( properties, Nodes.Comment );
                commentsIndex.getIndex().add( commentNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter posts( final BatchInserter batchInserter, final PostsBatchIndex postsIndex )
            throws FileNotFoundException
    {
        /*
        Post (documented)                                     
            id              int         unique              1   1       sequential       
            creationDate    dateTime    attribute           1   1                
            hasCreator      Person      directed relation   1   1                
            content         string      attribute           0   1                
            language        string      attribute           1   1   standard languages           
            imageFile       string      attribute           0   1   "photoXXX.jpg"           
            browserUsed     string      attribute           0   1   Chrome, IE, Firefox          
            locationIP      string      attribute           1   1                
            isLocatedIn     Country     directed relation   0   1                
            hasTag          Tag         directed relation   1   N                
            retweet         Post        directed relation   0   1            
        Post (in file - 2013/07/25)
            id              int         unique              1   1       sequential
            imageFile       string      attribute           0   1   "photoXXX.jpg"           
            creationDate    dateTime    attribute           1   1                
            locationIP      string      attribute           1   1                
            browserUsed     string      attribute           0   1   Chrome, IE, Firefox          
            content         string      attribute           0   1
            // TODO something else?                
         
        // TODO csv file has varying number of columns - bug?
        // TODO not documented in confluence
            id  imageFile   creationDate            locationIP      browserUsed     content
            00  photo0.jpg  2011-01-15T07:01:20Z    143.106.0.7     Firefox        
            100             2010-03-11T05:28:04Z    27.99.128.8     Firefox         About Michael Jordan, 1999, but returned... .
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "post.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                // TODO add the rest of the fields after file is repaired
                long postNodeId = batchInserter.createNode( properties, Nodes.Post );
                postsIndex.getIndex().add( postNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter persons( final BatchInserter batchInserter, final PersonsBatchIndex personsIndex )
            throws FileNotFoundException
    {
        /*
        Person (documented)
            id              int             unique              1   1                           sequential       
            creationDate    dateTime        attribute           1   1   startYear - endYear     random       
            firstName       string          attribute           1   1   dictionary                                      Person.IsLocatedIn   
            lastName        string          attribute           1   1   dictionary                                      Person.IsLocatedIn   
            gender          string          attribute           1   1   male/female (50%)       random       
            birthday        date            attribute           1   1   1980-1990               random       
            email           string          attribute           1   N                           top+random                                  F13
            speaks          string          attribute           1   N   languages            
            browserUsed     string          attribute           1   1   Chrone, IE, Firefox          
            locationIP      string          attribute           1   1                
            isLocatedIn     City            directed relation   1   1                           ?        
            isLocatedIn     Country         directed relation   1   1                           by country population                       F2,F9
            studyAt         Organization    directed relation   0   N                           University ranking      Person.IsLocatedIn  F3
            workAt          Organization    directed relation   0   N                                                   Person.IsLocatedIn  F10
            hasInterest     Tag             directed relation   1   N                           Singer's popularity     Person.IsLocatedIn  F5,F7
            likes           Post            directed relation   0   N                                                   Person.IsLocatedIn  F14
            knows           Person          relation            0   N                           power-law                                   F1
            follows         Person          directed relation   0   N
        Person (in file - 2013/07/25)                        
            id              int             unique              1   1                           sequential
            firstName       string          attribute           1   1   dictionary                                      Person.IsLocatedIn   
            lastName        string          attribute           1   1   dictionary                                      Person.IsLocatedIn   
            gender          string          attribute           1   1   male/female (50%)       random       
            birthday        date            attribute           1   1   1980-1990               random                          
            creationDate    dateTime        attribute           1   1   startYear - endYear     random
            locationIP      string          attribute           1   1                
            browserUsed     string          attribute           1   1   Chrone, IE, Firefox          

        // TODO confluence is inconsistent with this, last 2 columns (locationIP, browserUsed) are not documented
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
                long personNodeId = batchInserter.createNode( properties, Nodes.Person );
                personsIndex.getIndex().add( personNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter forums( final BatchInserter batchInserter, final ForumsBatchIndex forumIndex )
            throws FileNotFoundException
    {
        /*
        Forum (documented)
            id              int         unique              1   1   Person.id x 2   derivation      F4
            title           string      attribute           1   1                
            creationDate    dateTime    attribute           1   1                
            hasModerator    Person      directed relation   1   1                
            hasTag          Tag         directed relation   1   N                
            hasMember       Person      directed relation   1   N                
            containerOf     Post        directed relation   1   N
        Forum (in file - 2013/07/25)
            id              int         unique              1   1   Person.id x 2   derivation      F4
            title           string      attribute           1   1                
            creationDate    dateTime    attribute           1   1                

            // TODO not documented in confluence
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
                long forumNodeId = batchInserter.createNode( properties, Nodes.Forum );
                forumIndex.getIndex().add( forumNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter tags( final BatchInserter batchInserter, final TagsBatchIndex tagIndex )
            throws FileNotFoundException
    {
        /*
        Tag (documented)
            id      int         unique      1   1                
            name    string      attribute   1   1   dictionary
            hasType tagClass    attribute   1   N               
        Tag (in file - 2013/07/25)
            id      int         unique      1   1                
            name    string      attribute   1   1   dictionary
            hasType tagClass    attribute   1   N               

        // TODO inconsistent with confluence. confluence does not mention hasType
        id      name                hasType
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
                // TODO tagClass
                properties.put( "hasType", columnValues[2] );
                long tagNodeId = batchInserter.createNode( properties, Nodes.Tag );
                tagIndex.getIndex().add( tagNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter tagClasses( final BatchInserter batchInserter,
            final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        TagClass (documented)
            id          int         unique              1   1                
            name        string      attribute                        
            subClassOf  tagClass    directed relation                        
        TagClass (in file - 2013/07/25)
            id          int         unique              1   1                
            name        string      attribute                        
            subClassOf  tagClass    directed relation                        

        // TODO csv not documented in concluence
        id      name        subClassOf
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
                // TODO tagClass (this will be a relationship - perhaps don't
                // need to store)
                properties.put( "subClassOf", columnValues[2] );
                long tagClassNodeId = batchInserter.createNode( properties, Nodes.TagClass );
                tagClassesIndex.getIndex().add( tagClassNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter organisations( final BatchInserter batchInserter,
            final OrganisationsBatchIndex organisationsIndex ) throws FileNotFoundException
    {
        /*
        Organization (documented)
            id          int     unique              1   1                
            name        string  attribute           1   1   dictionary           
            isLocatedIn City    directed relation   1   1                
            isLocatedIn Country directed relation   1   1
        Organization (in file - 2013/07/25)                
            id          int     unique              1   1                
            name        string  attribute           1   1   dictionary
            // TODO what is the third column?           

        // TODO third column not documented in confluence
        id      name                            TODO what is this?
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
                // TODO what is this column?
                properties.put( "hasType", columnValues[2] );
                long organisationNodeId = batchInserter.createNode( properties, Nodes.Organisation );
                organisationsIndex.getIndex().add( organisationNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter languages( final BatchInserter batchInserter, final LanguagesBatchIndex languageIndex )
            throws FileNotFoundException
    {
        /*
        TODO csv is documented in confluence, but schema definition is not
        Language (documented)
            id          int     unique              1   1                
            name        string  attribute           1   1   dictionary           
        Language (in file - 2013/07/25)
            id          int     unique              1   1                
            name        string  attribute           1   1   dictionary           

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
                long languageNodeId = batchInserter.createNode( properties, Nodes.Language );
                languageIndex.getIndex().add( languageNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter locations( final BatchInserter batchInserter, final LocationsBatchIndex locationIndex )
            throws FileNotFoundException
    {
        /*
        TODO type not mentioned in confluence schema table, only in csv description
        
        Location (documented)
            id      int     unique      1   1                
            name    string  attribute   1   1   dbpedia
        Location (in file - 2013/07/25)
            id      int     unique      1   1                
            name    string  attribute   1   1   dbpedia

        TODO column 4 not documented in confluence at all
        id      name            type                                            ? TODO store as "second level label"?
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
                // TODO tagClass?
                properties.put( "hasType", columnValues[2] );
                // TODO string? "locationType"? finite set?
                properties.put( "locationType", columnValues[3] );
                long locationNodeId = batchInserter.createNode( properties, Nodes.Location );
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
                long emailAddressNodeId = batchInserter.createNode( properties, Nodes.EmailAddress );
                emailAddressIndex.getIndex().add( emailAddressNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter commentsRepliedToComments( final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex ) throws FileNotFoundException
    {
        /*
        Comment (documented)
            id              int         unique              1   1   sequential
            creationDate    dateTime    attribute           1   1
            hasCreator      Person      directed relation   1   1
            content         string      attribute           1   1
            browserUsed     string      attribute           0   1   Chrone, IE, Firefox
            locationIP      string      attribute           1   1
            isLocatedIn     Country     directed relation   0   1   Person.IsLocatedIn
            replyOf         Post        directed relation   0   1
            replyOf         Comment     directed relation   0   1
        Comment (in file - 2013/07/25)
            replyOf         Comment     directed relation   0   1
         
         id     from    to
         300    450     420
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
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Relationships.REPLY_OF, properties );
            }
        } );
    }

    private static CsvFileInserter commentsRepliedToPosts( final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        Comment (documented)
            id              int         unique              1   1   sequential
            creationDate    dateTime    attribute           1   1
            hasCreator      Person      directed relation   1   1
            content         string      attribute           1   1
            browserUsed     string      attribute           0   1   Chrome, IE, Firefox
            locationIP      string      attribute           1   1
            isLocatedIn     Country     directed relation   0   1   Person.IsLocatedIn
            replyOf         Post        directed relation   0   1
            replyOf         Comment     directed relation   0   1
        Comment (in file - 2013/07/25)
            replyOf         Post        directed relation   0   1
         
         id     from    to
         300    450     420
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
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Relationships.REPLY_OF, properties );
            }
        } );
    }

    private static CsvFileInserter commentsLocatedInLocation( final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex, final LocationsBatchIndex locationsIndex )
            throws FileNotFoundException
    {
        /*
        Comment (documented)
            id              int         unique              1   1   sequential
            creationDate    dateTime    attribute           1   1
            hasCreator      Person      directed relation   1   1
            content         string      attribute           1   1
            browserUsed     string      attribute           0   1   Chrone, IE, Firefox
            locationIP      string      attribute           1   1
            isLocatedIn     Country     directed relation   0   1   Person.IsLocatedIn
            replyOf         Post        directed relation   0   1
            replyOf         Comment     directed relation   0   1
        Comment (in file - 2013/07/25)
            isLocatedIn     Country     directed relation   0   1   Person.IsLocatedIn
         
         TODO csv not documented in confluence
         id     from_comment    to_location
         00     100             73
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
                        Relationships.IS_LOCATED_IN, properties );
            }
        } );
    }

    private static CsvFileInserter locationPartOfLocation( final BatchInserter batchInserter,
            final LocationsBatchIndex locationsIndex ) throws FileNotFoundException
    {
        /*
        TODO type not mentioned in confluence schema table, only in csv description
        
        Location (documented)
            id      int     unique      1   1                
            name    string  attribute   1   1   dbpedia
            TODO type?
        Location (in file - 2013/07/25)
            id      int     unique      1   1                
         
        TODO csv not documented in confluence
        id     from_location   to_location
        00      11              5170
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
                        Relationships.IS_PART_OF, properties );
            }
        } );
    }

    private static CsvFileInserter personKnowsPerson( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex ) throws FileNotFoundException
    {
        /*
        Person (documented)
            id              int             unique              1   1                           sequential       
            creationDate    dateTime        attribute           1   1   startYear - endYear     random       
            firstName       string          attribute           1   1   dictionary                                      Person.IsLocatedIn   
            lastName        string          attribute           1   1   dictionary                                      Person.IsLocatedIn   
            gender          string          attribute           1   1   male/female (50%)       random       
            birthday        date            attribute           1   1   1980-1990               random       
            email           string          attribute           1   N                           top+random                                  F13
            speaks          string          attribute           1   N   languages            
            browserUsed     string          attribute           1   1   Chrone, IE, Firefox          
            locationIP      string          attribute           1   1                
            isLocatedIn     City            directed relation   1   1                           ?        
            isLocatedIn     Country         directed relation   1   1                           by country population                       F2,F9
            studyAt         Organization    directed relation   0   N                           University ranking      Person.IsLocatedIn  F3
            workAt          Organization    directed relation   0   N                                                   Person.IsLocatedIn  F10
            hasInterest     Tag             directed relation   1   N                           Singer's popularity     Person.IsLocatedIn  F5,F7
            likes           Post            directed relation   0   N                                                   Person.IsLocatedIn  F14
            knows           Person          relation            0   N                           power-law                                   F1
            follows         Person          directed relation   0   N
        Person (in file - 2013/07/25)                        
            id              int             unique              1   1                           sequential       
            knows           Person          relation            0   N                           power-law                                   F1
         
        TODO csv not documented in confluence
        id      from_person     to_person
        00      11              5170
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
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Relationships.KNOWS,
                        properties );
            }
        } );
    }

    private static CsvFileInserter personStudiesAtOrganisation( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final OrganisationsBatchIndex organisationsIndex )
            throws FileNotFoundException
    {
        /*
        TODO relationship schema table does not specify attribute types
        studyAt 
        Person  Organization    classYear   gYear   1   1
        
        TODO csv description does not mention column 1, id
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
                                Relationships.STUDY_AT, properties );
                    }
                } );
    }

    private static CsvFileInserter personSpeaksLanguage( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final LanguagesBatchIndex languagesIndex )
            throws FileNotFoundException
    {
        /*
        TODO relationship schema table does not specify schema for SPEAKS relationship
        
        TODO csv description does not mention column 1, id
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
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Relationships.SPEAKS,
                        properties );
            }
        } );
    }

    private static CsvFileInserter personCreatorOfComment( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final CommentsBatchIndex commentsIndex ) throws FileNotFoundException
    {
        /*
        TODO relationship schema table does not specify schema for HAS_CREATOR relationship
        
        TODO csv description does not exist for person_creator_of_comment.csv
        
        // TODO guess based on assumption (comment)-[:HAS_CREATOR{id:_}]->(person)
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
                        Relationships.HAS_CREATOR, properties );
            }
        } );
    }

    private static CsvFileInserter personCreatorOfPost( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        TODO relationship schema table does not specify schema for HAS_CREATOR relationship
        
        TODO csv description does not exist for person_creator_of_post.csv
        
        // TODO guess based on assumption (post)-[:HAS_CREATOR{id:_}]->(person)
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
                        Relationships.HAS_CREATOR, properties );
            }
        } );
    }

    private static CsvFileInserter personModeratorOfForum( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final ForumsBatchIndex forumsIndex ) throws FileNotFoundException
    {
        /*
        TODO relationship schema table does not specify schema for HAS_MODERATOR relationship
        
        TODO csv description does not exist for person_moderator_of_forum.csv
        
        // TODO guess based on assumption (forum)-[:HAS_MODERATOR{id:_}]->(person)
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
                // remove after data generator fixed
                if ( columnValues == null ) return;

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[2], (Long) columnValues[1],
                        Relationships.HAS_MODERATOR, properties );
            }
        } );
    }

    private static CsvFileInserter personBasedNearLocation( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final LocationsBatchIndex locationsIndex )
            throws FileNotFoundException
    {
        /*
        Person (documented)
            id              int             unique              1   1                           sequential       
            isLocatedIn     City            directed relation   1   1                           ?        
            isLocatedIn     Country         directed relation   1   1                           by country population                       F2,F9
         */
        /*
        TODO relationship schema table does not specify schema for IS_LOCATED_IN relationship
        
        TODO csv description does not exist for person_based_near_location.csv
        
        // TODO guess based on assumption (person)-[:IS_LOCATED_IN{id:_}]->(location)
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
                        Relationships.IS_LOCATED_IN, properties );
            }
        } );
    }

    private static CsvFileInserter personWorksAtOrganisation( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final OrganisationsBatchIndex organisationsIndex )
            throws FileNotFoundException
    {
        /*
        workAt  
        Person  Organization    1   1
            workFrom    
            gYear   

        // TODO csv definition in confluence does not mention column 0 (id)
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
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Relationships.IS_LOCATED_IN, properties );
            }
        } );
    }

    private static CsvFileInserter personHasInterestTag( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition for HAS_INTEREST relationship

        TODO csv definition in confluence does not mention column 0 (id)
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
                        Relationships.HAS_INTEREST, properties );
            }
        } );
    }

    private static CsvFileInserter personHasEmailAddress( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final EmailAddressesBatchIndex emailAddressesIndex )
            throws FileNotFoundException
    {
        /*
        TODO no schema definition for HAS_EMAIL_ADDRESS relationship - though technically it's not a relationship in the schema

        TODO csv definition in confluence does not mention column 0 (id)
        has_email   
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
                                Relationships.HAS_EMAIL_ADDRESS, properties );
                    }
                } );
    }

    private static CsvFileInserter postHasTag( final BatchInserter batchInserter, final PostsBatchIndex postsIndex,
            final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition for HAS_TAG relationship - though technically it's not a relationship in the schema

        TODO not csv definition in confluence
        assume...
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
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Relationships.HAS_TAG, properties );
            }
        } );
    }

    private static CsvFileInserter postAnnotatedWithLanguage( final BatchInserter batchInserter,
            final PostsBatchIndex postsIndex, final LanguagesBatchIndex languagesIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition for ANNOTATED_WITH relationship - though technically it's not a relationship in the schema

        TODO no csv definition in confluence
        assume...
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
                        Relationships.ANNOTATED_WITH, properties );
            }
        } );
    }

    private static CsvFileInserter personLikesPost( final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        like    
        Person  Post    
            creationDate    dateTime
            1   1    

        TODO no csv definition in confluence
        assume...
        id  Post.id     Language.id

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
                // TODO dateTime
                properties.put( "creationDate", columnValues[3] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2], Relationships.LIKE,
                        properties );
            }
        } );
    }

    private static CsvFileInserter postLocatedAtLocation( final BatchInserter batchInserter,
            final PostsBatchIndex postsIndex, final LocationsBatchIndex locationsIndex ) throws FileNotFoundException
    {
        /*
        TODO no csv definition in confluence

         TODO csv not documented in confluence
         id     from_post    to_location
         00     100             73
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
                        Relationships.IS_LOCATED_IN, properties );
            }
        } );
    }

    private static CsvFileInserter forumHasMemberPerson( final BatchInserter batchInserter,
            final ForumsBatchIndex forumsIndex, final PersonsBatchIndex personsIndex ) throws FileNotFoundException
    {
        /*
        hasMember   
            Forum   Person  
            joinDate    dateTime    
            1   1

        TODO no csv definition in confluence

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
                    // TODO dateTime
                    properties.put( "joinDate", columnValues[3] );
                }
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Relationships.HAS_MEMBER, properties );
            }
        } );
    }

    private static CsvFileInserter forumContainerOfPost( final BatchInserter batchInserter,
            final ForumsBatchIndex forumsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition for CONTAINER_OF in confluence

        TODO no csv definition in confluence

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
                // remove after data generator fixed
                if ( columnValues == null ) return;

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[1], (Long) columnValues[2],
                        Relationships.CONTAINER_OF, properties );
            }
        } );
    }

    private static CsvFileInserter forumHasTag( final BatchInserter batchInserter, final ForumsBatchIndex forumsIndex,
            final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition for HAS_TAG in confluence

        TODO no csv definition in confluence

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
                // remove after data generator fixed
                if ( columnValues == null ) return;

                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "id", columnValues[0] );
                batchInserter.createRelationship( (Long) columnValues[2], (Long) columnValues[1],
                        Relationships.HAS_TAG, properties );
            }
        } );
    }

    private static CsvFileInserter tagHasTypeTagClass( final BatchInserter batchInserter,
            final TagsBatchIndex tagsIndex, final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition for HAS_TAG in confluence

        TODO no csv definition in confluence

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
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Relationships.HAS_TYPE, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter tagClassHasSubclassOfTagClass( final BatchInserter batchInserter,
            final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        TODO no schema definition for HAS_SUBCLASS_OF in confluence

        TODO no csv definition in confluence

        TODO assume parent/sub order as below
        
        parent_TagClass.id  sub_TagClass.id
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
                                Relationships.HAS_SUBCLASS_OF, EMPTY_MAP );
                    }
                } );
    }

    private static CsvFileInserter organisationBasedNearLocation( final BatchInserter batchInserter,
            final OrganisationsBatchIndex organisationsIndex, final LocationsBatchIndex locationsIndex )
            throws FileNotFoundException
    {
        /*
        TODO relationship schema table does not specify schema for IS_LOCATED_IN relationship

        TODO no csv definition in confluence for organisation_based_near_location.csv

        id      Organisation.id     Location.id
        17190   17190               3000
         */
        return new CsvFileInserter( new File( RAW_DATA_DIR + "organisation_based_near_location.csv" ),
                new CsvLineInserter()
                {
                    @Override
                    public void insert( Object[] columnValues )
                    {
                        Map<String, Object> properties = new HashMap<String, Object>();
                        properties.put( "id", Integer.parseInt( (String) columnValues[0] ) );
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
                            return;
                        }
                        batchInserter.createRelationship( fromOrganisationNodeId, toLocationNodeId,
                                Relationships.IS_LOCATED_IN, properties );
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
