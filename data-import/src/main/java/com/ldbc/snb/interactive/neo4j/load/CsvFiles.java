package com.ldbc.snb.interactive.neo4j.load;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashSet;
import java.util.Set;

public class CsvFiles {
    /*
     * Update Stream
     */
    public static final String UPDATE_STREAM = "updateStream_0.csv";

    /*
     * Nodes
     */

    public static final String COMMENT = "comment_0.csv";
    public static final String POST = "post_0.csv";
    public static final String PERSON = "person_0.csv";
    public static final String FORUM = "forum_0.csv";
    public static final String TAG = "tag_0.csv";
    public static final String TAGCLASS = "tagclass_0.csv";
    public static final String ORGANIZATION = "organisation_0.csv";
    public static final String PLACE = "place_0.csv";

    /*
     * Node Properties
     */
    public static final String PERSON_SPEAKS_LANGUAGE = "person_speaks_language_0.csv";
    public static final String PERSON_EMAIL_ADDRESS = "person_email_emailaddress_0.csv";
    /*
     * Relationships
     */
    public static final String COMMENT_REPLY_OF_COMMENT = "comment_replyOf_comment_0.csv";
    public static final String COMMENT_REPLY_OF_POST = "comment_replyOf_post_0.csv";
    public static final String COMMENT_LOCATED_IN_PLACE = "comment_isLocatedIn_place_0.csv";
    public static final String PLACE_IS_PART_OF_PLACE = "place_isPartOf_place_0.csv";
    public static final String PERSON_KNOWS_PERSON = "person_knows_person_0.csv";
    public static final String PERSON_STUDIES_AT_ORGANISATION = "person_studyAt_organisation_0.csv";
    public static final String COMMENT_HAS_CREATOR_PERSON = "comment_hasCreator_person_0.csv";
    public static final String POST_HAS_CREATOR_PERSON = "post_hasCreator_person_0.csv";
    public static final String FORUM_HAS_MODERATOR_PERSON = "forum_hasModerator_person_0.csv";
    public static final String PERSON_IS_LOCATED_IN_PLACE = "person_isLocatedIn_place_0.csv";
    public static final String PERSON_WORKS_AT_ORGANISATION = "person_workAt_organisation_0.csv";
    public static final String PERSON_HAS_INTEREST_TAG = "person_hasInterest_tag_0.csv";
    public static final String POST_HAS_TAG_TAG = "post_hasTag_tag_0.csv";
    public static final String PERSON_LIKES_POST = "person_likes_post_0.csv";
    public static final String POST_IS_LOCATED_IN_PLACE = "post_isLocatedIn_place_0.csv";
    public static final String FORUM_HAS_MEMBER_PERSON = "forum_hasMember_person_0.csv";
    public static final String FORUMS_CONTAINER_OF_POST = "forum_containerOf_post_0.csv";
    public static final String FORUM_HAS_TAG_TAG = "forum_hasTag_tag_0.csv";
    public static final String TAG_HAS_TYPE_TAGCLASS = "tag_hasType_tagclass_0.csv";
    public static final String TAGCLASS_IS_SUBCLASS_OF_TAGCLASS = "tagclass_isSubclassOf_tagclass_0.csv";
    public static final String ORGANISATION_IS_LOCATED_IN_PLACE = "organisation_isLocatedIn_place_0.csv";
    public static final String PERSON_LIKES_COMMENT = "person_likes_comment_0.csv";
    public static final String COMMENT_HAS_TAG_TAG = "comment_hasTag_tag_0.csv";

    public static Set<String> all(String csvDataDir) {
        File csvDataDirFile = new File(csvDataDir);
        FilenameFilter filenameFilter = new FilenameFilter() {
            public boolean accept(File directory, String fileName) {
                return fileName.endsWith(".csv");
            }
        };

        Set<String> files = new HashSet<String>();
        for (File csvFile : csvDataDirFile.listFiles(filenameFilter)) {
            files.add(csvFile.getAbsolutePath());
        }
        return files;
    }
}
