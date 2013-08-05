package com.ldbc.socialnet.neo4j.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import com.ldbc.socialnet.neo4j.CsvFileInserter;
import com.ldbc.socialnet.neo4j.CsvLineInserter;
import com.ldbc.socialnet.neo4j.domain.CommentsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.Domain;
import com.ldbc.socialnet.neo4j.domain.EmailAddressesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.ForumsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.LanguagesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.PlacesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.OrganisationsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.PersonsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.PostsBatchIndex;
import com.ldbc.socialnet.neo4j.domain.TagClassesBatchIndex;
import com.ldbc.socialnet.neo4j.domain.TagsBatchIndex;

public class CsvFileInserters
{
    private final static Map<String, Object> EMPTY_MAP = new HashMap<String, Object>();
    private final static Logger logger = Logger.getLogger( CsvFileInserters.class );

    public static List<CsvFileInserter> all( BatchInserter batchInserter,
            BatchInserterIndexProvider batchIndexProvider, String csvDataDir ) throws FileNotFoundException
    {
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
        PlacesBatchIndex placesIndex = new PlacesBatchIndex( batchIndexProvider );
        EmailAddressesBatchIndex emailAddressesIndex = new EmailAddressesBatchIndex( batchIndexProvider );

        /*
        * CSV Files
        */
        List<CsvFileInserter> fileInserters = new ArrayList<CsvFileInserter>();
        fileInserters.add( comments( csvDataDir, batchInserter, commentsIndex ) );
        fileInserters.add( forums( csvDataDir, batchInserter, forumsIndex ) );
        fileInserters.add( organisations( csvDataDir, batchInserter, organisationsIndex ) );
        fileInserters.add( persons( csvDataDir, batchInserter, personsIndex ) );
        fileInserters.add( places( csvDataDir, batchInserter, placesIndex ) );
        fileInserters.add( posts( csvDataDir, batchInserter, postsIndex ) );
        fileInserters.add( tagClasses( csvDataDir, batchInserter, tagClassesIndex ) );
        fileInserters.add( tags( csvDataDir, batchInserter, tagsIndex ) );
        fileInserters.add( commentHasCreatorPerson( csvDataDir, batchInserter, personsIndex, commentsIndex ) );
        fileInserters.add( commentIsLocatedInPlace( csvDataDir, batchInserter, commentsIndex, placesIndex ) );
        fileInserters.add( commentReplyOfComment( csvDataDir, batchInserter, commentsIndex ) );
        fileInserters.add( commentReplyOfPost( csvDataDir, batchInserter, commentsIndex, postsIndex ) );
        fileInserters.add( forumContainerOfPost( csvDataDir, batchInserter, forumsIndex, postsIndex ) );
        fileInserters.add( forumHasMemberPerson( csvDataDir, batchInserter, forumsIndex, personsIndex ) );
        fileInserters.add( forumHasModeratorPerson( csvDataDir, batchInserter, personsIndex, forumsIndex ) );
        fileInserters.add( forumHasTag( csvDataDir, batchInserter, forumsIndex, tagsIndex ) );
        fileInserters.add( personHasEmailAddress( csvDataDir, batchInserter, personsIndex, emailAddressesIndex ) );
        fileInserters.add( personHasInterestTag( csvDataDir, batchInserter, personsIndex, tagsIndex ) );
        fileInserters.add( personIsLocatedInPlace( csvDataDir, batchInserter, personsIndex, placesIndex ) );
        fileInserters.add( personKnowsPerson( csvDataDir, batchInserter, personsIndex ) );
        fileInserters.add( personLikesPost( csvDataDir, batchInserter, personsIndex, postsIndex ) );
        fileInserters.add( personSpeaksLanguage( csvDataDir, batchInserter, personsIndex, languagesIndex ) );
        fileInserters.add( personStudyAtOrganisation( csvDataDir, batchInserter, personsIndex, organisationsIndex ) );
        fileInserters.add( personWorksAtOrganisation( csvDataDir, batchInserter, personsIndex, organisationsIndex ) );
        fileInserters.add( placeIsPartOfPlace( csvDataDir, batchInserter, placesIndex ) );
        fileInserters.add( postHasCreatorPerson( csvDataDir, batchInserter, personsIndex, postsIndex ) );
        fileInserters.add( postHasTagTag( csvDataDir, batchInserter, postsIndex, tagsIndex ) );
        fileInserters.add( postIsLocatedInPlace( csvDataDir, batchInserter, postsIndex, placesIndex ) );
        fileInserters.add( tagClassIsSubclassOfTagClass( csvDataDir, batchInserter, tagClassesIndex ) );
        fileInserters.add( tagHasTypeTagClass( csvDataDir, batchInserter, tagsIndex, tagClassesIndex ) );
        fileInserters.add( organisationBasedNearPlace( csvDataDir, batchInserter, organisationsIndex, placesIndex ) );

        return fileInserters;
    }

    private static CsvFileInserter comments( final String csvDataDir, final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex ) throws FileNotFoundException
    {
        /*
        id  creationDate            location IP     browserUsed     content
        00  2010-03-11T10:11:18Z    14.134.0.11     Chrome          About Michael Jordan, Association...
         */
        return new CsvFileInserter( new File( csvDataDir + "comment.csv" ), new CsvLineInserter()
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

    private static CsvFileInserter posts( final String csvDataDir, final BatchInserter batchInserter,
            final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        id      imageFile   creationDate            locationIP      browserUsed     language    content
        100     photo9.jpg  2010-03-11T05:28:04Z    27.99.128.8     Firefox         zh          About Michael Jordan...
        */
        return new CsvFileInserter( new File( csvDataDir + "post.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "imageFile", columnValues[1] );
                // TODO datetime
                // 2010-12-28T07:16:25Z
                properties.put( "creationDate", columnValues[2] );
                properties.put( "locationIP", columnValues[3] );
                properties.put( "browserUsed", columnValues[4] );
                properties.put( "language", columnValues[5] );
                properties.put( "content", columnValues[6] );
                long postNodeId = batchInserter.createNode( properties, Domain.Node.POST );
                postsIndex.getIndex().add( postNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter persons( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex ) throws FileNotFoundException
    {
        /*
        id      firstName   lastName    gender  birthday    creationDate            locationIP      browserUsed
        75      Fernanda    Alves       male    1984-12-15  2010-12-14T11:41:37Z    143.106.0.7     Firefox
         */
        return new CsvFileInserter( new File( csvDataDir + "person.csv" ), new CsvLineInserter()
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
                // 1984-12-15
                properties.put( "birthday", columnValues[4] );
                // TODO datetime
                // 2010-12-28T07:16:25Z
                properties.put( "creationDate", columnValues[5] );
                properties.put( "locationIP", columnValues[6] );
                properties.put( "browserUsed", columnValues[7] );
                long personNodeId = batchInserter.createNode( properties, Domain.Node.PERSON );
                personsIndex.getIndex().add( personNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter forums( final String csvDataDir, final BatchInserter batchInserter,
            final ForumsBatchIndex forumIndex ) throws FileNotFoundException
    {
        /*
            id      title                       creationDate
            150     Wall of Fernanda Alves      2010-12-14T11:41:37Z
         */
        return new CsvFileInserter( new File( csvDataDir + "forum.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "title", columnValues[1] );
                // TODO datetime
                // 2010-12-28T07:16:25Z
                properties.put( "creationDate", columnValues[2] );
                long forumNodeId = batchInserter.createNode( properties, Domain.Node.FORUM );
                forumIndex.getIndex().add( forumNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter tags( final String csvDataDir, final BatchInserter batchInserter,
            final TagsBatchIndex tagIndex ) throws FileNotFoundException
    {
        /*
        id      name                url
        259     Gilberto_Gil        http://dbpedia.org/resource/Gilberto_Gil
         */
        return new CsvFileInserter( new File( csvDataDir + "tag.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                properties.put( "url", columnValues[2] );
                long tagNodeId = batchInserter.createNode( properties, Domain.Node.TAG );
                tagIndex.getIndex().add( tagNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter tagClasses( final String csvDataDir, final BatchInserter batchInserter,
            final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        id      name    url
        211     Person  http://dbpedia.org/ontology/Person
         */
        return new CsvFileInserter( new File( csvDataDir + "tagclass.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                properties.put( "url", columnValues[2] );
                long tagClassNodeId = batchInserter.createNode( properties, Domain.Node.TAG_CLASS );
                tagClassesIndex.getIndex().add( tagClassNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter organisations( final String csvDataDir, final BatchInserter batchInserter,
            final OrganisationsBatchIndex organisationsIndex ) throws FileNotFoundException
    {
        /*
        id  type        name                        url
        00  university  Universidade_de_Pernambuco  http://dbpedia.org/resource/Universidade_de_Pernambuco
         */
        return new CsvFileInserter( new File( csvDataDir + "organisation.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[2] );
                // TODO only necessary if connecting to dbpedia
                // properties.put( "url", columnValues[3] );
                long organisationNodeId = batchInserter.createNode( properties, Domain.Node.ORGANISATION,
                        Domain.OrganisationType.valueOf( ( (String) columnValues[1] ).toUpperCase() ) );
                organisationsIndex.getIndex().add( organisationNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter places( final String csvDataDir, final BatchInserter batchInserter,
            final PlacesBatchIndex placeIndex ) throws FileNotFoundException
    {
        /*
        id      name            url                                             type
        5170    South_America   http://dbpedia.org/resource/South_America       REGION
         */
        return new CsvFileInserter( new File( csvDataDir + "place.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                int id = Integer.parseInt( (String) columnValues[0] );
                properties.put( "id", id );
                properties.put( "name", columnValues[1] );
                properties.put( "url", columnValues[2] );
                long placeNodeId = batchInserter.createNode( properties, Domain.Node.PLACE,
                        Domain.PlaceType.valueOf( ( (String) columnValues[3] ).toUpperCase() ) );
                placeIndex.getIndex().add( placeNodeId, MapUtil.map( "id", id ) );
            }
        } );
    }

    private static CsvFileInserter commentReplyOfComment( final String csvDataDir, final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex ) throws FileNotFoundException
    {
        /*
        Comment.id  Comment.id
        20          00
         */
        return new CsvFileInserter( new File( csvDataDir + "comment_replyOf_comment.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long fromCommentNodeId = commentsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long toCommentNodeId = commentsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { fromCommentNodeId, toCommentNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.REPLY_OF,
                        EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter commentReplyOfPost( final String csvDataDir, final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        Comment.id  Post.id
        00          100
         */
        return new CsvFileInserter( new File( csvDataDir + "comment_replyOf_post.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long fromCommentNodeId = commentsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long toPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { fromCommentNodeId, toPostNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.REPLY_OF,
                        EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter commentIsLocatedInPlace( final String csvDataDir, final BatchInserter batchInserter,
            final CommentsBatchIndex commentsIndex, final PlacesBatchIndex placesIndex ) throws FileNotFoundException
    {
        /*
        Comment.id  Place.id
        100         73
         */
        return new CsvFileInserter( new File( csvDataDir + "comment_isLocatedIn_place.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long commentNodeId = commentsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long placeNodeId = placesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { commentNodeId, placeNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.IS_LOCATED_IN, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter placeIsPartOfPlace( final String csvDataDir, final BatchInserter batchInserter,
            final PlacesBatchIndex placesIndex ) throws FileNotFoundException
    {
        /*
        Place.id Place.id
        11          5170
         */
        return new CsvFileInserter( new File( csvDataDir + "place_isPartOf_place.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long fromPlaceNodeId = placesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long toPlaceNodeId = placesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { fromPlaceNodeId, toPlaceNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.IS_PART_OF, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter personKnowsPerson( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex ) throws FileNotFoundException
    {
        /*
        Person.id   Person.id
        75          1489
         */
        return new CsvFileInserter( new File( csvDataDir + "person_knows_person.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long toPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { fromPersonNodeId, toPersonNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.KNOWS,
                        EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter personStudyAtOrganisation( final String csvDataDir,
            final BatchInserter batchInserter, final PersonsBatchIndex personsIndex,
            final OrganisationsBatchIndex organisationsIndex ) throws FileNotFoundException
    {
        /*
        Person.id   Organisation.id classYear
        75          00                  2004
         */
        return new CsvFileInserter( new File( csvDataDir + "person_studyAt_organisation.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long toOrganisationNodeId = organisationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                int classYear = Integer.parseInt( (String) columnValues[2] );
                return new Object[] { fromPersonNodeId, toOrganisationNodeId, classYear };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "classYear", columnValues[2] );
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.STUDY_AT,
                        properties );
            }
        } );
    }

    private static CsvFileInserter personSpeaksLanguage( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final LanguagesBatchIndex languagesIndex )
            throws FileNotFoundException
    {
        /*        
        Person.id   language
        75          pt
         */
        return new CsvFileInserter( new File( csvDataDir + "person_speaks_language.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                return new Object[] { personNodeId, columnValues[1] };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.setNodeProperty( (Long) columnValues[0], "language", columnValues[1] );
            }
        } );
    }

    private static CsvFileInserter commentHasCreatorPerson( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final CommentsBatchIndex commentsIndex ) throws FileNotFoundException
    {
        /*        
        Comment.id  Person.id
        00          1402
         */
        return new CsvFileInserter( new File( csvDataDir + "comment_hasCreator_person.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long commentNodeId = commentsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { commentNodeId, personNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.HAS_CREATOR, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter postHasCreatorPerson( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        Post.id     Person.id
        00          75
         */
        return new CsvFileInserter( new File( csvDataDir + "post_hasCreator_person.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long postNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { postNodeId, personNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.HAS_CREATOR, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter forumHasModeratorPerson( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final ForumsBatchIndex forumsIndex ) throws FileNotFoundException
    {
        /*
        Forum.id    Person.id
        1500        75
         */
        return new CsvFileInserter( new File( csvDataDir + "forum_hasModerator_person.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long forumNodeId = 0;
                try
                {
                    forumNodeId = forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                }
                catch ( Exception e )
                {
                    /*
                    * TODO remove exception handling after data generator is
                    fixed
                    * usually ids in colummn 0 of forum.csv (and other .csv
                    files) have 0 suffix
                    * in forum.csv some rows do not, for example:
                    * 2978|Wall of Lei Liu|2010-03-11T03:55:32Z
                    * then files like person_moderator_of_forum.csv attempt to
                    retrieve 29780
                    */
                    // TODO uncomment to see broken ID's (still broken)
                    logger.error( "Forum node not found: " + columnValues[0] );
                    return null;
                }
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { forumNodeId, personNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove after data generator fixed
                if ( columnValues == null ) return;

                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.HAS_MODERATOR, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter personIsLocatedInPlace( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final PlacesBatchIndex placesIndex ) throws FileNotFoundException
    {
        /*        
        Person.id   Place.id
        75          310
         */
        return new CsvFileInserter( new File( csvDataDir + "person_isLocatedIn_place.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long placeNodeId = placesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { personNodeId, placeNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.IS_LOCATED_IN, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter personWorksAtOrganisation( final String csvDataDir,
            final BatchInserter batchInserter, final PersonsBatchIndex personsIndex,
            final OrganisationsBatchIndex organisationsIndex ) throws FileNotFoundException
    {
        /*
        Person.id   Organisation.id     workFrom
        75          10                  2016
         */
        return new CsvFileInserter( new File( csvDataDir + "person_workAt_organisation.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long organisationNodeId = organisationsIndex.getIndex().get( "id",
                        Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                int workFrom = Integer.parseInt( (String) columnValues[2] );
                return new Object[] { personNodeId, organisationNodeId, workFrom };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "workFrom", columnValues[2] );
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.WORKS_AT,
                        properties );
            }
        } );
    }

    private static CsvFileInserter personHasInterestTag( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        Person.id   Tag.id
        75          259
         */
        return new CsvFileInserter( new File( csvDataDir + "person_hasInterest_tag.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long tagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { personNodeId, tagNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.HAS_INTEREST, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter personHasEmailAddress( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final EmailAddressesBatchIndex emailAddressesIndex )
            throws FileNotFoundException
    {
        /*
        Person.id   email
        75          Fernanda75@gmx.com
         */
        return new CsvFileInserter( new File( csvDataDir + "person_email_emailaddress.csv" ), new CsvLineInserter()
        {
            @Override
            public void insert( Object[] columnValues )
            {
                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                batchInserter.setNodeProperty( personNodeId, "email", columnValues[1] );
            }
        } );
    }

    private static CsvFileInserter postHasTagTag( final String csvDataDir, final BatchInserter batchInserter,
            final PostsBatchIndex postsIndex, final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        Post.id Tag.id
        100     2903
         */
        return new CsvFileInserter( new File( csvDataDir + "post_hasTag_tag.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long postNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long tagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { postNodeId, tagNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO should Tag be a Label too?
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.HAS_TAG,
                        EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter personLikesPost( final String csvDataDir, final BatchInserter batchInserter,
            final PersonsBatchIndex personsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        Person.id   Post.id     creationDate
        1489        00          2011-01-20T11:18:41Z
         */
        return new CsvFileInserter( new File( csvDataDir + "person_likes_post.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long fromPersonNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long toPostNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                // TODO dateTime
                Object creationDate = columnValues[2];
                return new Object[] { fromPersonNodeId, toPostNodeId, creationDate };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "creationDate", columnValues[2] );
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.LIKES,
                        properties );
            }
        } );
    }

    private static CsvFileInserter postIsLocatedInPlace( final String csvDataDir, final BatchInserter batchInserter,
            final PostsBatchIndex postsIndex, final PlacesBatchIndex placesIndex ) throws FileNotFoundException
    {
        /*
        Post.id     Place.id
        00          11
         */
        return new CsvFileInserter( new File( csvDataDir + "post_isLocatedIn_place.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long postNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long placeNodeId = placesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { postNodeId, placeNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.IS_LOCATED_IN, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter forumHasMemberPerson( final String csvDataDir, final BatchInserter batchInserter,
            final ForumsBatchIndex forumsIndex, final PersonsBatchIndex personsIndex ) throws FileNotFoundException
    {
        /*
        Forum.id    Person.id   joinDate
        150         1489        2011-01-02T01:01:10Z        
         */
        return new CsvFileInserter( new File( csvDataDir + "forum_hasMember_person.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long forumNodeId = 0;
                try
                {
                    forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                }
                catch ( Exception e )
                {
                    /*
                     * TODO remove exception handling after data generator is fixed
                     */
                    // TODO uncomment to see broken IDs (still broken)
                    logger.error( "Forum not found: " + columnValues[0] );
                    return null;
                }

                long personNodeId = personsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                Object joinDate = columnValues[2];
                return new Object[] { forumNodeId, personNodeId, joinDate };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove after data generator fixed
                if ( columnValues == null ) return;
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put( "joinDate", columnValues[2] );
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.HAS_MEMBER, properties );
            }
        } );
    }

    private static CsvFileInserter forumContainerOfPost( final String csvDataDir, final BatchInserter batchInserter,
            final ForumsBatchIndex forumsIndex, final PostsBatchIndex postsIndex ) throws FileNotFoundException
    {
        /*
        Forum.id    Post.id
        40220       00
         */
        return new CsvFileInserter( new File( csvDataDir + "forum_containerOf_post.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long forumNodeId = 0;
                try
                {
                    forumNodeId = forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
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
                    // TODO uncomment to see broken IDs (still broken)
                    logger.error( "Forum not found: " + columnValues[0] );
                    return null;
                }
                long postNodeId = postsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { forumNodeId, postNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove after data generator fixed
                if ( columnValues == null ) return;
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                        Domain.Rel.CONTAINER_OF, EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter forumHasTag( final String csvDataDir, final BatchInserter batchInserter,
            final ForumsBatchIndex forumsIndex, final TagsBatchIndex tagsIndex ) throws FileNotFoundException
    {
        /*
        Forum.id    Tag.id
        75          259
         */
        return new CsvFileInserter( new File( csvDataDir + "forum_hasTag_tag.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long forumNodeId = 0;
                try
                {
                    forumNodeId = forumsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
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
                    // TODO uncomment to see broken IDs (still broken)
                    logger.error( "Forum not found: " + columnValues[0] );
                    return null;
                }
                long tagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { tagNodeId, forumNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                // TODO remove after data generator fixed
                if ( columnValues == null ) return;
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.HAS_TAG,
                        EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter tagHasTypeTagClass( final String csvDataDir, final BatchInserter batchInserter,
            final TagsBatchIndex tagsIndex, final TagClassesBatchIndex tagClassesIndex ) throws FileNotFoundException
    {
        /*
        Tag.id  TagClass.id
        259     211
         */
        return new CsvFileInserter( new File( csvDataDir + "tag_hasType_tagclass.csv" ), new CsvLineInserter()
        {
            @Override
            public Object[] transform( Object[] columnValues )
            {
                long tagNodeId = tagsIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                long tagClassNodeId = tagClassesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                return new Object[] { tagNodeId, tagClassNodeId };
            }

            @Override
            public void insert( Object[] columnValues )
            {
                batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1], Domain.Rel.HAS_TYPE,
                        EMPTY_MAP );
            }
        } );
    }

    private static CsvFileInserter tagClassIsSubclassOfTagClass( final String csvDataDir,
            final BatchInserter batchInserter, final TagClassesBatchIndex tagClassesIndex )
            throws FileNotFoundException
    {
        /*
        TagClass.id     TagClass.id
        211             239
         */
        return new CsvFileInserter( new File( csvDataDir + "tagclass_isSubclassOf_tagclass.csv" ),
                new CsvLineInserter()
                {
                    @Override
                    public Object[] transform( Object[] columnValues )
                    {
                        long subTagClassNodeId = tagClassesIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                        long tagClassNodeId = tagClassesIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                        return new Object[] { subTagClassNodeId, tagClassNodeId };
                    }

                    @Override
                    public void insert( Object[] columnValues )
                    {
                        batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                                Domain.Rel.IS_SUBCLASS_OF, EMPTY_MAP );
                    }
                } );
    }

    private static CsvFileInserter organisationBasedNearPlace( final String csvDataDir,
            final BatchInserter batchInserter, final OrganisationsBatchIndex organisationsIndex,
            final PlacesBatchIndex placesIndex ) throws FileNotFoundException
    {
        /*
        Organisation.id     Place.id
        00                  301
         */
        return new CsvFileInserter( new File( csvDataDir + "organisation_isLocatedIn_place.csv" ),
                new CsvLineInserter()
                {
                    @Override
                    public Object[] transform( Object[] columnValues )
                    {
                        long organisationNodeId = organisationsIndex.getIndex().get( "id",
                                Integer.parseInt( (String) columnValues[0] ) ).getSingle();
                        long placeNodeId = 0;
                        try
                        {
                            placeNodeId = placesIndex.getIndex().get( "id", Integer.parseInt( (String) columnValues[1] ) ).getSingle();
                        }
                        catch ( Exception e )
                        {
                            /*
                             * TODO remove exception handling after generator fixed
                             * Place.id column contains ids that are not in place.csv
                             * eg. 301
                             */
                            // TODO uncomment to see broken IDs (still broken)
                            logger.error( "Place not found: " + columnValues[1] );
                            return null;
                        }
                        return new Object[] { organisationNodeId, placeNodeId };
                    }

                    @Override
                    public void insert( Object[] columnValues )
                    {
                        // TODO remove when generator fixed
                        if ( columnValues == null ) return;
                        batchInserter.createRelationship( (Long) columnValues[0], (Long) columnValues[1],
                                Domain.Rel.IS_LOCATED_IN, EMPTY_MAP );
                    }
                } );
    }

}
