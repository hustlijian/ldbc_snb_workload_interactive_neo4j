package com.ldbc.socialnet.neo4j.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashSet;
import java.util.Set;

public class CsvFiles
{
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
