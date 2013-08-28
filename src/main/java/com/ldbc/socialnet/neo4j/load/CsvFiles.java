package com.ldbc.socialnet.neo4j.load;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashSet;
import java.util.Set;

public class CsvFiles
{
    /*
     * Nodes
     */

    public static final String COMMENT = "comment.csv";
    public static final String POST = "post.csv";
    public static final String PERSON = "person.csv";
    public static final String FORUM = "forum.csv";
    public static final String TAG = "tag.csv";
    public static final String TAGCLASS = "tagclass.csv";
    public static final String ORGANISATION = "organisation.csv";
    public static final String PLACE = "place.csv";

    /*
     * Node Properties
     */
    public static final String PERSON_SPEAKS_LANGUAGE = "person_speaks_language.csv";
    public static final String PERSON_EMAIL_ADDRESS = "person_email_emailaddress.csv";
    /*
     * Relationships
     */
    public static final String COMMENT_REPLY_OF_COMMENT = "comment_replyOf_comment.csv";
    public static final String COMMENT_REPLY_OF_POST = "comment_replyOf_post.csv";
    public static final String COMMENT_LOCATED_IN_PLACE = "comment_isLocatedIn_place.csv";
    public static final String PLACE_IS_PART_OF_PLACE = "place_isPartOf_place.csv";
    public static final String PERSON_KNOWS_PERSON = "person_knows_person.csv";
    public static final String PERSON_STUDIES_AT_ORGANISATION = "person_studyAt_organisation.csv";
    public static final String COMMENT_HAS_CREATOR_PERSON = "comment_hasCreator_person.csv";
    public static final String POST_HAS_CREATOR_PERSON = "post_hasCreator_person.csv";
    public static final String FORUM_HAS_MODERATOR_PERSON = "forum_hasModerator_person.csv";
    public static final String PERSON_IS_LOCATED_IN_PLACE = "person_isLocatedIn_place.csv";
    public static final String PERSON_WORKS_AT_ORGANISATION = "person_workAt_organisation.csv";
    public static final String PERSON_HAS_INTEREST_TAG = "person_hasInterest_tag.csv";
    public static final String POST_HAS_TAG_TAG = "post_hasTag_tag.csv";
    public static final String PERSON_LIKES_POST = "person_likes_post.csv";
    public static final String POST_IS_LOCATED_IN_PLACE = "post_isLocatedIn_place.csv";
    public static final String FORUM_HAS_MEMBER_PERSON = "forum_hasMember_person.csv";
    public static final String FORUMS_CONTAINER_OF_POST = "forum_containerOf_post.csv";
    public static final String FORUM_HAS_TAG_TAG = "forum_hasTag_tag.csv";
    public static final String TAG_HAS_TYPE_TAGCLASS = "tag_hasType_tagclass.csv";
    public static final String TAGCLASS_IS_SUBCLASS_OF_TAGCLASS = "tagclass_isSubclassOf_tagclass.csv";
    public static final String ORGANISATION_IS_LOCATED_IN_PLACE = "organisation_isLocatedIn_place.csv";

    public static Set<String> all( String csvDataDir )
    {
        File csvDataDirFile = new File( csvDataDir );
        FilenameFilter filenameFilter = new FilenameFilter()
        {
            public boolean accept( File directory, String fileName )
            {
                return fileName.endsWith( ".csv" );
            }
        };

        Set<String> files = new HashSet<String>();
        for ( File csvFile : csvDataDirFile.listFiles( filenameFilter ) )
        {
            files.add( csvFile.getAbsolutePath() );
        }

        // files.add( csvDataDir + "comment.csv" );
        // files.add( csvDataDir + "comment_hasCreator_person.csv" );
        // files.add( csvDataDir + "comment_isLocatedIn_location.csv" );
        // files.add( csvDataDir + "comment_replyOf_comment.csv" );
        // files.add( csvDataDir + "comment_replyOf_post.csv" );
        // files.add( csvDataDir + "forum.csv" );
        // files.add( csvDataDir + "forum_container_of_post.csv" );
        // files.add( csvDataDir + "forum_hasMember_person.csv" );
        // files.add( csvDataDir + "forum_hasModerator_person.csv" );
        // files.add( csvDataDir + "forum_hasTag_tag.csv" );
        // files.add( csvDataDir + "location.csv" );
        // files.add( csvDataDir + "location_partOf_location.csv" );
        // files.add( csvDataDir + "organisation.csv" );
        // files.add( csvDataDir + "organisation_isLocatedIn_location.csv" );
        // files.add( csvDataDir + "person.csv" );
        // files.add( csvDataDir + "person_hasEmail_emailaddress.csv" );
        // files.add( csvDataDir + "person_hasInterest_tag.csv" );
        // files.add( csvDataDir + "person_isLocatedIn_location.csv" );
        // files.add( csvDataDir + "person_knows_person.csv" );
        // files.add( csvDataDir + "person_likes_post.csv" );
        // files.add( csvDataDir + "person_speaks_language.csv" );
        // files.add( csvDataDir + "person_studyAt_organisation.csv" );
        // files.add( csvDataDir + "person_workAt_organisation.csv" );
        // files.add( csvDataDir + "post.csv" );
        // files.add( csvDataDir + "post_hasCreator_person.csv" );
        // files.add( csvDataDir + "post_hasTag_tag.csv" );
        // files.add( csvDataDir + "post_isLocatedIn_location.csv" );
        // files.add( csvDataDir + "tag.csv" );
        // files.add( csvDataDir + "tagclass.csv" );
        // files.add( csvDataDir + "tagclass_isSubclassOf_tagclass.csv" );
        // files.add( csvDataDir + "tag_hasType_tagclass.csv" );
        return files;
    }
}
