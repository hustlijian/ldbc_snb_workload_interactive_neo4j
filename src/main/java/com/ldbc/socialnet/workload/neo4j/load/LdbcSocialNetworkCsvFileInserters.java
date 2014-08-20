package com.ldbc.socialnet.workload.neo4j.load;

import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.load.tempindex.TempIndexFactory;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.apache.log4j.Logger;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class LdbcSocialNetworkCsvFileInserters {
    private static final Logger logger = Logger.getLogger(LdbcSocialNetworkCsvFileInserters.class);

    private final static Map<String, Object> EMPTY_MAP = new HashMap<String, Object>();
    private final static String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private final static SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);
    private final static String DATE_FORMAT_STRING = "yyyy-MM-dd";
    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STRING);

    private final CommentsTempIndex commentsIndex;
    private final PostsTempIndex postsIndex;
    private final PersonsTempIndex personsIndex;
    private final ForumsTempIndex forumsIndex;
    private final TagsTempIndex tagsIndex;
    private final TagClassesTempIndex tagClassesIndex;
    private final OrganisationsTempIndex organisationsIndex;
    private final PlacesTempIndex placesIndex;

    private final CsvFileInserter commentsInserter;
    private final CsvFileInserter forumsInserter;
    private final CsvFileInserter organisationsInserter;
    private final CsvFileInserter personsInserter;
    private final CsvFileInserter placesInserter;
    private final CsvFileInserter postsInserter;
    private final CsvFileInserter tagClassesInserter;
    private final CsvFileInserter tagsInserter;
    private final CsvFileInserter commentHasCreatorPersonInserter;
    private final CsvFileInserter commentIsLocatedInPlaceInserter;
    private final CsvFileInserter commentReplyOfCommentInserter;
    private final CsvFileInserter commentReplyOfPostInserter;
    private final CsvFileInserter forumContainerOfPostInserter;
    private final CsvFileInserter forumHasMemberPersonInserter;
    private final CsvFileInserter forumHasModeratorPersonInserter;
    private final CsvFileInserter forumHasTagInserter;
    private final CsvFileInserter personHasEmailAddressInserter;
    private final CsvFileInserter personHasInterestTagInserter;
    private final CsvFileInserter personIsLocatedInPlaceInserter;
    private final CsvFileInserter personKnowsPersonInserter;
    private final CsvFileInserter personLikesPostInserter;
    private final CsvFileInserter personSpeaksLanguageInserter;
    private final CsvFileInserter personStudyAtOrganisationInserter;
    private final CsvFileInserter personWorksAtOrganisationInserter;
    private final CsvFileInserter placeIsPartOfPlaceInserter;
    private final CsvFileInserter postHasCreatorPersonInserter;
    private final CsvFileInserter postHasTagTagInserter;
    private final CsvFileInserter postIsLocatedInPlaceInserter;
    private final CsvFileInserter tagClassIsSubclassOfTagClassInserter;
    private final CsvFileInserter tagHasTypeTagClassInserter;
    private final CsvFileInserter organisationBasedNearPlaceInserter;

    public LdbcSocialNetworkCsvFileInserters(TempIndexFactory<Long, Long> tempIndexFactory,
                                             final BatchInserter batchInserter, String csvDataDir) throws FileNotFoundException {
        /*
         * Temp Node ID Indexes
         */
        this.commentsIndex = new CommentsTempIndex(tempIndexFactory.create());
        this.postsIndex = new PostsTempIndex(tempIndexFactory.create());
        this.personsIndex = new PersonsTempIndex(tempIndexFactory.create());
        this.forumsIndex = new ForumsTempIndex(tempIndexFactory.create());
        this.tagsIndex = new TagsTempIndex(tempIndexFactory.create());
        this.tagClassesIndex = new TagClassesTempIndex(tempIndexFactory.create());
        this.organisationsIndex = new OrganisationsTempIndex(tempIndexFactory.create());
        this.placesIndex = new PlacesTempIndex(tempIndexFactory.create());

        /*
         * Node File Inserters
         */
        this.commentsInserter = comments(csvDataDir, batchInserter, commentsIndex);
        this.forumsInserter = forums(csvDataDir, batchInserter, forumsIndex);
        this.organisationsInserter = organisations(csvDataDir, batchInserter, organisationsIndex);
        this.personsInserter = persons(csvDataDir, batchInserter, personsIndex);
        this.placesInserter = places(csvDataDir, batchInserter, placesIndex);
        this.postsInserter = posts(csvDataDir, batchInserter, postsIndex);
        this.tagClassesInserter = tagClasses(csvDataDir, batchInserter, tagClassesIndex);
        this.tagsInserter = tags(csvDataDir, batchInserter, tagsIndex);

        /*
         * Property File Inserters
         */
        this.personHasEmailAddressInserter = personHasEmailAddress(csvDataDir, batchInserter, personsIndex);
        this.personSpeaksLanguageInserter = personSpeaksLanguage(csvDataDir, batchInserter, personsIndex);

        /*
         * Relationship File Inserters
         */
        this.commentHasCreatorPersonInserter = commentHasCreatorPerson(csvDataDir, batchInserter, personsIndex,
                commentsIndex);
        this.commentIsLocatedInPlaceInserter = commentIsLocatedInPlace(csvDataDir, batchInserter, commentsIndex,
                placesIndex);
        this.commentReplyOfCommentInserter = commentReplyOfComment(csvDataDir, batchInserter, commentsIndex);
        this.commentReplyOfPostInserter = commentReplyOfPost(csvDataDir, batchInserter, commentsIndex, postsIndex);
        this.forumContainerOfPostInserter = forumContainerOfPost(csvDataDir, batchInserter, forumsIndex, postsIndex);
        this.forumHasMemberPersonInserter = forumHasMemberPerson(csvDataDir, batchInserter, forumsIndex, personsIndex);
        this.forumHasModeratorPersonInserter = forumHasModeratorPerson(csvDataDir, batchInserter, personsIndex,
                forumsIndex);
        this.forumHasTagInserter = forumHasTag(csvDataDir, batchInserter, forumsIndex, tagsIndex);
        this.personHasInterestTagInserter = personHasInterestTag(csvDataDir, batchInserter, personsIndex, tagsIndex);
        this.personIsLocatedInPlaceInserter = personIsLocatedInPlace(csvDataDir, batchInserter, personsIndex,
                placesIndex);
        this.personKnowsPersonInserter = personKnowsPerson(csvDataDir, batchInserter, personsIndex);
        this.personLikesPostInserter = personLikesPost(csvDataDir, batchInserter, personsIndex, postsIndex);
        this.personStudyAtOrganisationInserter = personStudyAtOrganisation(csvDataDir, batchInserter, personsIndex,
                organisationsIndex);
        this.personWorksAtOrganisationInserter = personWorksAtOrganisation(csvDataDir, batchInserter, personsIndex,
                organisationsIndex);
        this.placeIsPartOfPlaceInserter = placeIsPartOfPlace(csvDataDir, batchInserter, placesIndex);
        this.postHasCreatorPersonInserter = postHasCreatorPerson(csvDataDir, batchInserter, personsIndex, postsIndex);
        this.postHasTagTagInserter = postHasTagTag(csvDataDir, batchInserter, postsIndex, tagsIndex);
        this.postIsLocatedInPlaceInserter = postIsLocatedInPlace(csvDataDir, batchInserter, postsIndex, placesIndex);
        this.tagClassIsSubclassOfTagClassInserter = tagClassIsSubclassOfTagClass(csvDataDir, batchInserter,
                tagClassesIndex);
        this.tagHasTypeTagClassInserter = tagHasTypeTagClass(csvDataDir, batchInserter, tagsIndex, tagClassesIndex);
        this.organisationBasedNearPlaceInserter = organisationBasedNearPlace(csvDataDir, batchInserter,
                organisationsIndex, placesIndex);
    }

    public CommentsTempIndex getCommentsIndex() {
        return commentsIndex;
    }

    public PostsTempIndex getPostsIndex() {
        return postsIndex;
    }

    public PersonsTempIndex getPersonsIndex() {
        return personsIndex;
    }

    public ForumsTempIndex getForumsIndex() {
        return forumsIndex;
    }

    public TagsTempIndex getTagsIndex() {
        return tagsIndex;
    }

    public TagClassesTempIndex getTagClassesIndex() {
        return tagClassesIndex;
    }

    public OrganisationsTempIndex getOrganisationsIndex() {
        return organisationsIndex;
    }

    public PlacesTempIndex getPlacesIndex() {
        return placesIndex;
    }

    public CsvFileInserter getCommentsInserter() {
        return commentsInserter;
    }

    public CsvFileInserter getForumsInserter() {
        return forumsInserter;
    }

    public CsvFileInserter getOrganisationsInserter() {
        return organisationsInserter;
    }

    public CsvFileInserter getPersonsInserter() {
        return personsInserter;
    }

    public CsvFileInserter getPlacesInserter() {
        return placesInserter;
    }

    public CsvFileInserter getPostsInserter() {
        return postsInserter;
    }

    public CsvFileInserter getTagClassesInserter() {
        return tagClassesInserter;
    }

    public CsvFileInserter getTagsInserter() {
        return tagsInserter;
    }

    public CsvFileInserter getCommentHasCreatorPersonInserter() {
        return commentHasCreatorPersonInserter;
    }

    public CsvFileInserter getCommentIsLocatedInPlaceInserter() {
        return commentIsLocatedInPlaceInserter;
    }

    public CsvFileInserter getCommentReplyOfCommentInserter() {
        return commentReplyOfCommentInserter;
    }

    public CsvFileInserter getCommentReplyOfPostInserter() {
        return commentReplyOfPostInserter;
    }

    public CsvFileInserter getForumContainerOfPostInserter() {
        return forumContainerOfPostInserter;
    }

    public CsvFileInserter getForumHasMemberPersonInserter() {
        return forumHasMemberPersonInserter;
    }

    public CsvFileInserter getForumHasModeratorPersonInserter() {
        return forumHasModeratorPersonInserter;
    }

    public CsvFileInserter getForumHasTagInserter() {
        return forumHasTagInserter;
    }

    public CsvFileInserter getPersonHasEmailAddressInserter() {
        return personHasEmailAddressInserter;
    }

    public CsvFileInserter getPersonHasInterestTagInserter() {
        return personHasInterestTagInserter;
    }

    public CsvFileInserter getPersonIsLocatedInPlaceInserter() {
        return personIsLocatedInPlaceInserter;
    }

    public CsvFileInserter getPersonKnowsPersonInserter() {
        return personKnowsPersonInserter;
    }

    public CsvFileInserter getPersonLikesPostInserter() {
        return personLikesPostInserter;
    }

    public CsvFileInserter getPersonSpeaksLanguageInserter() {
        return personSpeaksLanguageInserter;
    }

    public CsvFileInserter getPersonStudyAtOrganisationInserter() {
        return personStudyAtOrganisationInserter;
    }

    public CsvFileInserter getPersonWorksAtOrganisationInserter() {
        return personWorksAtOrganisationInserter;
    }

    public CsvFileInserter getPlaceIsPartOfPlaceInserter() {
        return placeIsPartOfPlaceInserter;
    }

    public CsvFileInserter getPostHasCreatorPersonInserter() {
        return postHasCreatorPersonInserter;
    }

    public CsvFileInserter getPostHasTagTagInserter() {
        return postHasTagTagInserter;
    }

    public CsvFileInserter getPostIsLocatedInPlaceInserter() {
        return postIsLocatedInPlaceInserter;
    }

    public CsvFileInserter getTagClassIsSubclassOfTagClassInserter() {
        return tagClassIsSubclassOfTagClassInserter;
    }

    public CsvFileInserter getTagHasTypeTagClassInserter() {
        return tagHasTypeTagClassInserter;
    }

    public CsvFileInserter getOrganisationBasedNearPlaceInserter() {
        return organisationBasedNearPlaceInserter;
    }

    private static CsvFileInserter comments(final String csvDataDir, final BatchInserter batchInserter,
                                            final CommentsTempIndex commentsIndex) throws FileNotFoundException {
        /*
        id  creationDate            location IP     browserUsed     content
        00  2010-03-11T10:11:18Z    14.134.0.11     Chrome          About Michael Jordan, Association...
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.COMMENT), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                // TODO remove?
                // properties.put( Domain.Comment.ID, id );
                String creationDateString = (String) columnValues[1];
                try {
                    // 2010-12-28T07:16:25Z
                    Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
                    properties.put(Domain.Message.CREATION_DATE, creationDate.getTime());
                } catch (ParseException e) {
                    long now = System.currentTimeMillis();
                    properties.put(Domain.Message.CREATION_DATE, now);
                    logger.error(String.format("Invalid DateTime string: %s\nSet creationDate to now instead\n%s",
                            creationDateString, e));
                }
                properties.put(Domain.Message.LOCATION_IP, columnValues[2]);
                properties.put(Domain.Message.BROWSER_USED, columnValues[3]);
                properties.put(Domain.Message.CONTENT, columnValues[4]);
                long commentNodeId = batchInserter.createNode(properties, Domain.Nodes.Comment);
                commentsIndex.put(id, commentNodeId);
            }
        });
    }

    private static CsvFileInserter posts(final String csvDataDir, final BatchInserter batchInserter,
                                         final PostsTempIndex postsIndex) throws FileNotFoundException {
        /*
        id      imageFile   creationDate            locationIP      browserUsed     language    content
        100     photo9.jpg  2010-03-11T05:28:04Z    27.99.128.8     Firefox         zh          About Michael Jordan...
        */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.POST), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                properties.put(Domain.Message.ID, id);
                properties.put(Domain.Post.IMAGE_FILE, columnValues[1]);
                String creationDateString = (String) columnValues[2];
                try {
                    // 2010-12-28T07:16:25Z
                    Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
                    properties.put(Domain.Message.CREATION_DATE, creationDate.getTime());
                } catch (ParseException e) {
                    long now = System.currentTimeMillis();
                    properties.put(Domain.Message.CREATION_DATE, now);
                    logger.error(String.format("Invalid DateTime string: %s\nSet creationDate to now instead\n%s",
                            creationDateString, e));
                }
                properties.put(Domain.Message.LOCATION_IP, columnValues[3]);
                properties.put(Domain.Message.BROWSER_USED, columnValues[4]);
                properties.put(Domain.Post.LANGUAGE, columnValues[5]);
                properties.put(Domain.Message.CONTENT, columnValues[6]);
                long postNodeId = batchInserter.createNode(properties, Domain.Nodes.Post);
                postsIndex.put(id, postNodeId);
            }
        });
    }

    private static Set<Long> allPersonIds = new HashSet<>();
    private static Long allPersonIdsCount = 0L;
    private static Long minPersonId = Long.MAX_VALUE;
    private static Long maxPersonId = Long.MIN_VALUE;

    private static CsvFileInserter persons(final String csvDataDir, final BatchInserter batchInserter,
                                           final PersonsTempIndex personsIndex) throws FileNotFoundException {
        /*
        id      firstName   lastName    gender  birthday    creationDate            locationIP      browserUsed
        75      Fernanda    Alves       male    1984-12-15  2010-12-14T11:41:37Z    143.106.0.7     Firefox
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                properties.put(Domain.Person.ID, id);
                properties.put(Domain.Person.FIRST_NAME, columnValues[1]);
                properties.put(Domain.Person.LAST_NAME, columnValues[2]);
                properties.put(Domain.Person.GENDER, columnValues[3]);
                String birthdayString = (String) columnValues[4];
                try {
                    Date birthday = DATE_FORMAT.parse(birthdayString);
                    properties.put(Domain.Person.BIRTHDAY, birthday.getTime());
                } catch (ParseException e) {
                    long now = System.currentTimeMillis();
                    properties.put(Domain.Person.BIRTHDAY, now);
                    logger.error(String.format("Invalid Date string: %s\nSet birthday to now instead\n%s",
                            birthdayString, e));
                }
                String creationDateString = (String) columnValues[5];
                try {
                    Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
                    properties.put(Domain.Person.CREATION_DATE, creationDate.getTime());
                } catch (ParseException e) {
                    long now = System.currentTimeMillis();
                    properties.put(Domain.Person.CREATION_DATE, now);
                    logger.error(String.format("Invalid DateTime string: %s\nSet creationDate to now instead\n%s",
                            creationDateString, e));
                }
                properties.put(Domain.Person.LOCATION_IP, columnValues[6]);
                properties.put(Domain.Person.BROWSER_USED, columnValues[7]);
                properties.put(Domain.Person.EMAIL_ADDRESSES, new String[0]);
                properties.put(Domain.Person.LANGUAGES, new String[0]);
                long personNodeId = batchInserter.createNode(properties, Domain.Nodes.Person);
                personsIndex.put(id, personNodeId);
            }
        });
    }

    private static CsvFileInserter forums(final String csvDataDir, final BatchInserter batchInserter,
                                          final ForumsTempIndex forumIndex) throws FileNotFoundException {
        /*
            id      title                       creationDate
            150     Wall of Fernanda Alves      2010-12-14T11:41:37Z
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.FORUM), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                // TODO remove?
                // properties.put( "id", id );
                properties.put(Domain.Forum.TITLE, columnValues[1]);
                String creationDateString = (String) columnValues[2];
                try {
                    Date creationDate = DATE_TIME_FORMAT.parse(creationDateString);
                    properties.put(Domain.Forum.CREATION_DATE, creationDate.getTime());
                } catch (ParseException e) {
                    long now = System.currentTimeMillis();
                    properties.put(Domain.Forum.CREATION_DATE, now);
                    logger.error(String.format("Invalid DateTime string: %s\nSet creationDate to now instead\n%s",
                            creationDateString, e));
                }
                long forumNodeId = batchInserter.createNode(properties, Domain.Nodes.Forum);
                forumIndex.put(id, forumNodeId);
            }
        });
    }

    private static CsvFileInserter tags(final String csvDataDir, final BatchInserter batchInserter,
                                        final TagsTempIndex tagIndex) throws FileNotFoundException {
        /*
        id      name                url
        259     Gilberto_Gil        http://dbpedia.org/resource/Gilberto_Gil
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.TAG), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                // TODO remove?
                // properties.put( "id", id );
                properties.put(Domain.Tag.NAME, columnValues[1]);
                properties.put(Domain.Tag.URI, columnValues[2]);
                long tagNodeId = batchInserter.createNode(properties, Domain.Nodes.Tag);
                tagIndex.put(id, tagNodeId);
            }
        });
    }

    private static CsvFileInserter tagClasses(final String csvDataDir, final BatchInserter batchInserter,
                                              final TagClassesTempIndex tagClassesIndex) throws FileNotFoundException {
        /*
        id      name    url
        211     Person  http://dbpedia.org/ontology/Person
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.TAGCLASS), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                // TODO remove?
                // properties.put( "id", id );
                properties.put(Domain.TagClass.NAME, columnValues[1]);
                properties.put(Domain.TagClass.URI, columnValues[2]);
                long tagClassNodeId = batchInserter.createNode(properties, Domain.Nodes.TagClass);
                tagClassesIndex.put(id, tagClassNodeId);
            }
        });
    }

    private static CsvFileInserter organisations(final String csvDataDir, final BatchInserter batchInserter,
                                                 final OrganisationsTempIndex organisationsIndex) throws FileNotFoundException {
        /*
        id  type        name                        url
        00  university  Universidade_de_Pernambuco  http://dbpedia.org/resource/Universidade_de_Pernambuco
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.ORGANISATION), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                // TODO remove?
                // properties.put( "id", id );
                properties.put(Domain.Organisation.NAME, columnValues[2]);
                // TODO only necessary if connecting to dbpedia
                // properties.put( "url", columnValues[3] );
                long organisationNodeId = batchInserter.createNode(properties, stringToOrganisationType((String) columnValues[1]));
                organisationsIndex.put(id, organisationNodeId);
            }

            Domain.Organisation.Type stringToOrganisationType(String organisationTypeString) {
                if (organisationTypeString.toLowerCase().equals("university"))
                    return Domain.Organisation.Type.University;
                if (organisationTypeString.toLowerCase().equals("company"))
                    return Domain.Organisation.Type.Company;
                throw new RuntimeException("Unknown organisation type: " + organisationTypeString);
            }

        });
    }

    private static CsvFileInserter places(final String csvDataDir, final BatchInserter batchInserter,
                                          final PlacesTempIndex placeIndex) throws FileNotFoundException {
        /*
        id      name            url                                             type
        5170    South_America   http://dbpedia.org/resource/South_America       REGION
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PLACE), new CsvLineInserter() {
            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                long id = Long.parseLong((String) columnValues[0]);
                // TODO remove?
                // properties.put( "id", id );
                properties.put(Domain.Place.NAME, columnValues[1]);
                properties.put(Domain.Place.URI, columnValues[2]);
                Domain.Place.Type placeType = stringToPlaceType((String) columnValues[3]);
                long placeNodeId = batchInserter.createNode(properties, placeType);
                placeIndex.put(id, placeNodeId);
            }

            Domain.Place.Type stringToPlaceType(String placeTypeString) {
                if (placeTypeString.toLowerCase().equals("city")) return Domain.Place.Type.City;
                if (placeTypeString.toLowerCase().equals("country")) return Domain.Place.Type.Country;
                if (placeTypeString.toLowerCase().equals("continent")) return Domain.Place.Type.Continent;
                throw new RuntimeException("Unknown place type: " + placeTypeString);
            }
        });
    }

    private static CsvFileInserter commentReplyOfComment(final String csvDataDir, final BatchInserter batchInserter,
                                                         final CommentsTempIndex commentsIndex) throws FileNotFoundException {
        /*
        Comment.id  Comment.id
        20          00
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.COMMENT_REPLY_OF_COMMENT), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long fromCommentNodeId = commentsIndex.get(Long.parseLong((String) columnValues[0]));
                long toCommentNodeId = commentsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{fromCommentNodeId, toCommentNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1], Domain.Rels.REPLY_OF,
                        EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter commentReplyOfPost(final String csvDataDir, final BatchInserter batchInserter,
                                                      final CommentsTempIndex commentsIndex, final PostsTempIndex postsIndex) throws FileNotFoundException {
        /*
        Comment.id  Post.id
        00          100
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.COMMENT_REPLY_OF_POST), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long fromCommentNodeId = commentsIndex.get(Long.parseLong((String) columnValues[0]));
                long toPostNodeId = postsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{fromCommentNodeId, toPostNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1], Domain.Rels.REPLY_OF,
                        EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter commentIsLocatedInPlace(final String csvDataDir, final BatchInserter batchInserter,
                                                           final CommentsTempIndex commentsIndex, final PlacesTempIndex placesIndex) throws FileNotFoundException {
        /*
        Comment.id  Place.id
        100         73
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.COMMENT_LOCATED_IN_PLACE), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long commentNodeId = commentsIndex.get(Long.parseLong((String) columnValues[0]));
                long placeNodeId = placesIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{commentNodeId, placeNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.IS_LOCATED_IN, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter placeIsPartOfPlace(final String csvDataDir, final BatchInserter batchInserter,
                                                      final PlacesTempIndex placesIndex) throws FileNotFoundException {
        /*
        Place.id Place.id
        11          5170
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PLACE_IS_PART_OF_PLACE), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long fromPlaceNodeId = placesIndex.get(Long.parseLong((String) columnValues[0]));
                long toPlaceNodeId = placesIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{fromPlaceNodeId, toPlaceNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.IS_PART_OF, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter personKnowsPerson(final String csvDataDir, final BatchInserter batchInserter,
                                                     final PersonsTempIndex personsIndex) throws FileNotFoundException {
        /*
        Person.id   Person.id
        75          1489
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_KNOWS_PERSON), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long fromPersonNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                long toPersonNodeId = personsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{fromPersonNodeId, toPersonNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                long fromPerson = (long) columnValues[0];
                long toPerson = (long) columnValues[1];
                // CSV contains bidirectional KNOWS rels but only 1 necessary
                if (fromPerson < toPerson) {
                    batchInserter.createRelationship(fromPerson, toPerson, Domain.Rels.KNOWS, EMPTY_MAP);
                }
            }
        });
    }

    private static CsvFileInserter personStudyAtOrganisation(final String csvDataDir,
                                                             final BatchInserter batchInserter, final PersonsTempIndex personsIndex,
                                                             final OrganisationsTempIndex organisationsIndex) throws FileNotFoundException {
        /*
        Person.id   Organisation.id classYear
        75          00                  2004
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_STUDIES_AT_ORGANISATION),
                new CsvLineInserter() {
                    @Override
                    public Object[] transform(Object[] columnValues) {
                        long fromPersonNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                        long toOrganisationNodeId = organisationsIndex.get(Long.parseLong((String) columnValues[1]));
                        int classYear = Integer.parseInt((String) columnValues[2]);
                        return new Object[]{fromPersonNodeId, toOrganisationNodeId, classYear};
                    }

                    @Override
                    public void insert(Object[] columnValues) {
                        Map<String, Object> properties = new HashMap<String, Object>();
                        properties.put(Domain.StudiesAt.CLASS_YEAR, columnValues[2]);
                        batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                                Domain.Rels.STUDY_AT, properties);
                    }
                });
    }

    private static CsvFileInserter personSpeaksLanguage(final String csvDataDir, final BatchInserter batchInserter,
                                                        final PersonsTempIndex personsIndex) throws FileNotFoundException {
        /*        
        Person.id   language
        75          pt
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_SPEAKS_LANGUAGE), new CsvLineInserter() {
            /*
             * TODO file needs to have better format
             * updating nodes, especially resizing arrays, does not encourage vendors to load big datasets
             */
            @Override
            public void insert(Object[] columnValues) {
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                Map<String, Object> personNodeProperties = batchInserter.getNodeProperties(personNodeId);
                String[] languages = (String[]) personNodeProperties.get(Domain.Person.LANGUAGES);
                String newLanguage = (String) columnValues[1];
                String[] langaugesPlusNewLanguage = Utils.copyArrayAndAddElement(languages, newLanguage);
                batchInserter.setNodeProperty(personNodeId, Domain.Person.LANGUAGES, langaugesPlusNewLanguage);
            }
        });
    }

    private static CsvFileInserter commentHasCreatorPerson(final String csvDataDir, final BatchInserter batchInserter,
                                                           final PersonsTempIndex personsIndex, final CommentsTempIndex commentsIndex) throws FileNotFoundException {
        /*        
        Comment.id  Person.id
        00          1402
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.COMMENT_HAS_CREATOR_PERSON), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long commentNodeId = commentsIndex.get(Long.parseLong((String) columnValues[0]));
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{commentNodeId, personNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.HAS_CREATOR, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter postHasCreatorPerson(final String csvDataDir, final BatchInserter batchInserter,
                                                        final PersonsTempIndex personsIndex, final PostsTempIndex postsIndex) throws FileNotFoundException {
        /*
        Post.id     Person.id
        00          75
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.POST_HAS_CREATOR_PERSON), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long postNodeId = postsIndex.get(Long.parseLong((String) columnValues[0]));
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{postNodeId, personNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.HAS_CREATOR, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter forumHasModeratorPerson(final String csvDataDir, final BatchInserter batchInserter,
                                                           final PersonsTempIndex personsIndex, final ForumsTempIndex forumsIndex) throws FileNotFoundException {
        /*
        Forum.id    Person.id
        1500        75
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.FORUM_HAS_MODERATOR_PERSON), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long forumNodeId = forumsIndex.get(Long.parseLong((String) columnValues[0]));
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{forumNodeId, personNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.HAS_MODERATOR, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter personIsLocatedInPlace(final String csvDataDir, final BatchInserter batchInserter,
                                                          final PersonsTempIndex personsIndex, final PlacesTempIndex placesIndex) throws FileNotFoundException {
        /*        
        Person.id   Place.id
        75          310
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_IS_LOCATED_IN_PLACE), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                long placeNodeId = placesIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{personNodeId, placeNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.IS_LOCATED_IN, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter personWorksAtOrganisation(final String csvDataDir,
                                                             final BatchInserter batchInserter, final PersonsTempIndex personsIndex,
                                                             final OrganisationsTempIndex organisationsIndex) throws FileNotFoundException {
        /*
        Person.id   Organisation.id     workFrom
        75          10                  2016
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_WORKS_AT_ORGANISATION),
                new CsvLineInserter() {
                    @Override
                    public Object[] transform(Object[] columnValues) {
                        long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                        long organisationNodeId = organisationsIndex.get(Long.parseLong((String) columnValues[1]));
                        int workFrom = Integer.parseInt((String) columnValues[2]);
                        return new Object[]{personNodeId, organisationNodeId, workFrom};
                    }

                    @Override
                    public void insert(Object[] columnValues) {
                        Map<String, Object> properties = new HashMap<String, Object>();
                        properties.put(Domain.WorksAt.WORK_FROM, columnValues[2]);
                        batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                                Domain.Rels.WORKS_AT, properties);
                    }
                });
    }

    private static CsvFileInserter personHasInterestTag(final String csvDataDir, final BatchInserter batchInserter,
                                                        final PersonsTempIndex personsIndex, final TagsTempIndex tagsIndex) throws FileNotFoundException {
        /*
        Person.id   Tag.id
        75          259
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_HAS_INTEREST_TAG), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                long tagNodeId = tagsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{personNodeId, tagNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.HAS_INTEREST, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter personHasEmailAddress(final String csvDataDir, final BatchInserter batchInserter,
                                                         final PersonsTempIndex personsIndex) throws FileNotFoundException {
        /*
        Person.id   email
        75          Fernanda75@gmx.com
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_EMAIL_ADDRESS), new CsvLineInserter() {
            /*
             * TODO file needs to have better format
             * updating nodes, especially resizing arrays, does not encourage vendors to load big datasets
             */
            @Override
            public void insert(Object[] columnValues) {
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                Map<String, Object> personNodeProperties = batchInserter.getNodeProperties(personNodeId);
                String[] emailAddresses = (String[]) personNodeProperties.get(Domain.Person.EMAIL_ADDRESSES);
                String newEmailAddress = (String) columnValues[1];
                String[] emailAddressPlusNewAddress = Utils.copyArrayAndAddElement(emailAddresses, newEmailAddress);
                batchInserter.setNodeProperty(personNodeId, Domain.Person.EMAIL_ADDRESSES, emailAddressPlusNewAddress);
            }
        });
    }

    private static CsvFileInserter postHasTagTag(final String csvDataDir, final BatchInserter batchInserter,
                                                 final PostsTempIndex postsIndex, final TagsTempIndex tagsIndex) throws FileNotFoundException {
        /*
        Post.id Tag.id
        100     2903
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.POST_HAS_TAG_TAG), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long postNodeId = postsIndex.get(Long.parseLong((String) columnValues[0]));
                long tagNodeId = tagsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{postNodeId, tagNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1], Domain.Rels.HAS_TAG,
                        EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter personLikesPost(final String csvDataDir, final BatchInserter batchInserter,
                                                   final PersonsTempIndex personsIndex, final PostsTempIndex postsIndex) throws FileNotFoundException {
        /*
        Person.id   Post.id     creationDate
        1489        00          2011-01-20T11:18:41Z
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.PERSON_LIKES_POST), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long fromPersonNodeId = personsIndex.get(Long.parseLong((String) columnValues[0]));
                long toPostNodeId = postsIndex.get(Long.parseLong((String) columnValues[1]));
                String creationDateString = (String) columnValues[2];
                long creationDateAsTime;
                try {
                    creationDateAsTime = DATE_TIME_FORMAT.parse(creationDateString).getTime();
                } catch (ParseException e) {
                    long now = System.currentTimeMillis();
                    creationDateAsTime = now;
                    logger.error(String.format("Invalid DateTime string: %s\nSet creationDate to now instead\n%s",
                            creationDateString, e));
                }
                return new Object[]{fromPersonNodeId, toPostNodeId, creationDateAsTime};
            }

            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put(Domain.Likes.CREATION_DATE, columnValues[2]);
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1], Domain.Rels.LIKES,
                        properties);
            }
        });
    }

    private static CsvFileInserter postIsLocatedInPlace(final String csvDataDir, final BatchInserter batchInserter,
                                                        final PostsTempIndex postsIndex, final PlacesTempIndex placesIndex) throws FileNotFoundException {
        /*
        Post.id     Place.id
        00          11
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.POST_IS_LOCATED_IN_PLACE), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long postNodeId = postsIndex.get(Long.parseLong((String) columnValues[0]));
                long placeNodeId = placesIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{postNodeId, placeNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.IS_LOCATED_IN, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter forumHasMemberPerson(final String csvDataDir, final BatchInserter batchInserter,
                                                        final ForumsTempIndex forumsIndex, final PersonsTempIndex personsIndex) throws FileNotFoundException {
        /*
        Forum.id    Person.id   joinDate
        150         1489        2011-01-02T01:01:10Z        
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.FORUM_HAS_MEMBER_PERSON), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long forumNodeId = forumsIndex.get(Long.parseLong((String) columnValues[0]));
                long personNodeId = personsIndex.get(Long.parseLong((String) columnValues[1]));
                String joinDateString = (String) columnValues[2];
                long joinDateAsTime;
                try {
                    joinDateAsTime = DATE_TIME_FORMAT.parse(joinDateString).getTime();
                } catch (ParseException e) {
                    long now = System.currentTimeMillis();
                    joinDateAsTime = now;
                    logger.error(String.format("Invalid DateTime string: %s\nSet joinDate to now instead\n%s",
                            joinDateString, e));
                }
                return new Object[]{forumNodeId, personNodeId, joinDateAsTime};
            }

            @Override
            public void insert(Object[] columnValues) {
                Map<String, Object> properties = new HashMap<String, Object>();
                properties.put(Domain.HasMember.JOIN_DATE, columnValues[2]);
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.HAS_MEMBER, properties);
            }
        });
    }

    private static CsvFileInserter forumContainerOfPost(final String csvDataDir, final BatchInserter batchInserter,
                                                        final ForumsTempIndex forumsIndex, final PostsTempIndex postsIndex) throws FileNotFoundException {
        /*
        Forum.id    Post.id
        40220       00
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.FORUMS_CONTAINER_OF_POST), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long forumNodeId = forumsIndex.get(Long.parseLong((String) columnValues[0]));
                long postNodeId = postsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{forumNodeId, postNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                        Domain.Rels.CONTAINER_OF, EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter forumHasTag(final String csvDataDir, final BatchInserter batchInserter,
                                               final ForumsTempIndex forumsIndex, final TagsTempIndex tagsIndex) throws FileNotFoundException {
        /*
        Forum.id    Tag.id
        75          259
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.FORUM_HAS_TAG_TAG), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long forumNodeId = forumsIndex.get(Long.parseLong((String) columnValues[0]));
                long tagNodeId = tagsIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{tagNodeId, forumNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1], Domain.Rels.HAS_TAG,
                        EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter tagHasTypeTagClass(final String csvDataDir, final BatchInserter batchInserter,
                                                      final TagsTempIndex tagsIndex, final TagClassesTempIndex tagClassesIndex) throws FileNotFoundException {
        /*
        Tag.id  TagClass.id
        259     211
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.TAG_HAS_TYPE_TAGCLASS), new CsvLineInserter() {
            @Override
            public Object[] transform(Object[] columnValues) {
                long tagNodeId = tagsIndex.get(Long.parseLong((String) columnValues[0]));
                long tagClassNodeId = tagClassesIndex.get(Long.parseLong((String) columnValues[1]));
                return new Object[]{tagNodeId, tagClassNodeId};
            }

            @Override
            public void insert(Object[] columnValues) {
                batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1], Domain.Rels.HAS_TYPE,
                        EMPTY_MAP);
            }
        });
    }

    private static CsvFileInserter tagClassIsSubclassOfTagClass(final String csvDataDir,
                                                                final BatchInserter batchInserter, final TagClassesTempIndex tagClassesIndex) throws FileNotFoundException {
        /*
        TagClass.id     TagClass.id
        211             239
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS),
                new CsvLineInserter() {
                    @Override
                    public Object[] transform(Object[] columnValues) {
                        long subTagClassNodeId = tagClassesIndex.get(Long.parseLong((String) columnValues[0]));
                        long tagClassNodeId = tagClassesIndex.get(Long.parseLong((String) columnValues[1]));
                        return new Object[]{subTagClassNodeId, tagClassNodeId};
                    }

                    @Override
                    public void insert(Object[] columnValues) {
                        batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                                Domain.Rels.IS_SUBCLASS_OF, EMPTY_MAP);
                    }
                });
    }

    private static CsvFileInserter organisationBasedNearPlace(final String csvDataDir,
                                                              final BatchInserter batchInserter, final OrganisationsTempIndex organisationsIndex,
                                                              final PlacesTempIndex placesIndex) throws FileNotFoundException {
        /*
        Organisation.id     Place.id
        00                  301
         */
        return new CsvFileInserter(new File(csvDataDir + CsvFiles.ORGANISATION_IS_LOCATED_IN_PLACE),
                new CsvLineInserter() {
                    @Override
                    public Object[] transform(Object[] columnValues) {
                        long organisationNodeId = organisationsIndex.get(Long.parseLong((String) columnValues[0]));
                        long placeNodeId = placesIndex.get(Long.parseLong((String) columnValues[1]));
                        return new Object[]{organisationNodeId, placeNodeId};
                    }

                    @Override
                    public void insert(Object[] columnValues) {
                        batchInserter.createRelationship((long) columnValues[0], (long) columnValues[1],
                                Domain.Rels.IS_LOCATED_IN, EMPTY_MAP);
                    }
                });
    }

    private static class RelationshipIdsComparator implements Comparator<Object[]> {
        @Override
        public int compare(Object[] o1, Object[] o2) {
            long minRelOfo1 = Math.min((long) o1[0], (long) o1[1]);
            long minRelOfo2 = Math.min((long) o2[0], (long) o2[1]);
            if (minRelOfo1 < minRelOfo2) return -1;
            if (minRelOfo1 > minRelOfo2) return 1;
            return 0;
        }
    }
}
