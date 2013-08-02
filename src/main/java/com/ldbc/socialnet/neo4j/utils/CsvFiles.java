package com.ldbc.socialnet.neo4j.utils;

import java.util.HashSet;
import java.util.Set;

public class CsvFiles
{
    private final static String RAW_DATA_DIR = "/home/alex/workspace/java/ldbc_socialnet_bm/ldbc_socialnet_dbgen/outputDir/";

    public static Set<String> all()
    {
        Set<String> files = new HashSet<String>();
        files.add( RAW_DATA_DIR + "comment.csv" );
        files.add( RAW_DATA_DIR + "comment_hasCreator_person.csv" );
        files.add( RAW_DATA_DIR + "comment_isLocatedIn_location.csv" );
        files.add( RAW_DATA_DIR + "comment_replyOf_comment.csv" );
        files.add( RAW_DATA_DIR + "comment_replyOf_post.csv" );
        files.add( RAW_DATA_DIR + "forum.csv" );
        files.add( RAW_DATA_DIR + "forum_container_of_post.csv" );
        files.add( RAW_DATA_DIR + "forum_hasMember_person.csv" );
        files.add( RAW_DATA_DIR + "forum_hasModerator_person.csv" );
        files.add( RAW_DATA_DIR + "forum_hasTag_tag.csv" );
        files.add( RAW_DATA_DIR + "location.csv" );
        files.add( RAW_DATA_DIR + "location_partOf_location.csv" );
        files.add( RAW_DATA_DIR + "organisation.csv" );
        files.add( RAW_DATA_DIR + "organisation_isLocatedIn_location.csv" );
        files.add( RAW_DATA_DIR + "person.csv" );
        files.add( RAW_DATA_DIR + "person_hasEmail_emailaddress.csv" );
        files.add( RAW_DATA_DIR + "person_hasInterest_tag.csv" );
        files.add( RAW_DATA_DIR + "person_isLocatedIn_location.csv" );
        files.add( RAW_DATA_DIR + "person_knows_person.csv" );
        files.add( RAW_DATA_DIR + "person_likes_post.csv" );
        files.add( RAW_DATA_DIR + "person_speaks_language.csv" );
        files.add( RAW_DATA_DIR + "person_studyAt_organisation.csv" );
        files.add( RAW_DATA_DIR + "person_workAt_organisation.csv" );
        files.add( RAW_DATA_DIR + "post.csv" );
        files.add( RAW_DATA_DIR + "post_hasCreator_person.csv" );
        files.add( RAW_DATA_DIR + "post_hasTag_tag.csv" );
        files.add( RAW_DATA_DIR + "post_isLocatedIn_location.csv" );
        files.add( RAW_DATA_DIR + "tag.csv" );
        files.add( RAW_DATA_DIR + "tagclass.csv" );
        files.add( RAW_DATA_DIR + "tagclass_isSubclassOf_tagclass.csv" );
        files.add( RAW_DATA_DIR + "tag_hasType_tagclass.csv" );
        return files;
    }

}
