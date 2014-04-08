package com.ldbc.socialnet.neo4j.utils;

import com.ldbc.socialnet.workload.neo4j.load.CsvFiles;
import com.ldbc.socialnet.workload.neo4j.utils.Config;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UtilsTests {
    @Test
    public void shouldCopyAndAppendElementToNewArrayWhenOldArrayNotNull() throws IOException {
        String[] oldArray = {"1", "2", "3"};
        String newElement = "4";
        String[] newArray = Utils.copyArrayAndAddElement(oldArray, newElement);
        assertThat(newArray, equalTo(new String[]{"1", "2", "3", "4"}));
    }

    @Test
    public void shouldCopyAndAppendElementToNewArrayWhenOldArrayNull() throws IOException {
        String[] oldArray = null;
        String newElement = "4";
        String[] newArray = Utils.copyArrayAndAddElement(oldArray, newElement);
        assertThat(newArray, equalTo(new String[]{"4"}));
    }

    @Test
    public void shouldBeNoDifferenceBetweenAllCsvFilesAndConstantDefinedCsvFiles() throws IOException {
        // Given
        String csvDir = new File(Config.DATA_DIR).getAbsolutePath() + "/";

        /*
         * Nodes
         */
        Set<String> constantDefinedCsvFiles = new HashSet<String>();
        constantDefinedCsvFiles.add(csvDir + CsvFiles.COMMENT);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.POST);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.FORUM);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.TAG);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.TAGCLASS);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.ORGANISATION);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PLACE);

        /*
         * Node Properties
         */
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_SPEAKS_LANGUAGE);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_EMAIL_ADDRESS);
        /*
         * Relationships
         */
        constantDefinedCsvFiles.add(csvDir + CsvFiles.COMMENT_REPLY_OF_COMMENT);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.COMMENT_REPLY_OF_POST);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.COMMENT_LOCATED_IN_PLACE);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PLACE_IS_PART_OF_PLACE);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_KNOWS_PERSON);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_STUDIES_AT_ORGANISATION);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.COMMENT_HAS_CREATOR_PERSON);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.POST_HAS_CREATOR_PERSON);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.FORUM_HAS_MODERATOR_PERSON);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_IS_LOCATED_IN_PLACE);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_WORKS_AT_ORGANISATION);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_HAS_INTEREST_TAG);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.POST_HAS_TAG_TAG);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_LIKES_POST);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.POST_IS_LOCATED_IN_PLACE);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.FORUM_HAS_MEMBER_PERSON);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.FORUMS_CONTAINER_OF_POST);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.FORUM_HAS_TAG_TAG);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.TAG_HAS_TYPE_TAGCLASS);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.ORGANISATION_IS_LOCATED_IN_PLACE);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.PERSON_LIKES_COMMENT);
        constantDefinedCsvFiles.add(csvDir + CsvFiles.COMMENT_HAS_TAG_TAG);

        // When

        // Then
        assertThat(constantDefinedCsvFiles, is(CsvFiles.all(Config.DATA_DIR)));

    }
}
