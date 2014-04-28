package com.ldbc.socialnet.neo4j.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.socialnet.workload.neo4j.load.CsvFiles;
import com.ldbc.socialnet.workload.neo4j.utils.Config;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
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
        HashSet<String> defined = new HashSet<String>();
        defined.add(csvDir + CsvFiles.COMMENT);
        defined.add(csvDir + CsvFiles.POST);
        defined.add(csvDir + CsvFiles.PERSON);
        defined.add(csvDir + CsvFiles.FORUM);
        defined.add(csvDir + CsvFiles.TAG);
        defined.add(csvDir + CsvFiles.TAGCLASS);
        defined.add(csvDir + CsvFiles.ORGANISATION);
        defined.add(csvDir + CsvFiles.PLACE);

        /*
         * Node Properties
         */
        defined.add(csvDir + CsvFiles.PERSON_SPEAKS_LANGUAGE);
        defined.add(csvDir + CsvFiles.PERSON_EMAIL_ADDRESS);
        /*
         * Relationships
         */
        defined.add(csvDir + CsvFiles.COMMENT_REPLY_OF_COMMENT);
        defined.add(csvDir + CsvFiles.COMMENT_REPLY_OF_POST);
        defined.add(csvDir + CsvFiles.COMMENT_LOCATED_IN_PLACE);
        defined.add(csvDir + CsvFiles.PLACE_IS_PART_OF_PLACE);
        defined.add(csvDir + CsvFiles.PERSON_KNOWS_PERSON);
        defined.add(csvDir + CsvFiles.PERSON_STUDIES_AT_ORGANISATION);
        defined.add(csvDir + CsvFiles.COMMENT_HAS_CREATOR_PERSON);
        defined.add(csvDir + CsvFiles.POST_HAS_CREATOR_PERSON);
        defined.add(csvDir + CsvFiles.FORUM_HAS_MODERATOR_PERSON);
        defined.add(csvDir + CsvFiles.PERSON_IS_LOCATED_IN_PLACE);
        defined.add(csvDir + CsvFiles.PERSON_WORKS_AT_ORGANISATION);
        defined.add(csvDir + CsvFiles.PERSON_HAS_INTEREST_TAG);
        defined.add(csvDir + CsvFiles.POST_HAS_TAG_TAG);
        defined.add(csvDir + CsvFiles.PERSON_LIKES_POST);
        defined.add(csvDir + CsvFiles.POST_IS_LOCATED_IN_PLACE);
        defined.add(csvDir + CsvFiles.FORUM_HAS_MEMBER_PERSON);
        defined.add(csvDir + CsvFiles.FORUMS_CONTAINER_OF_POST);
        defined.add(csvDir + CsvFiles.FORUM_HAS_TAG_TAG);
        defined.add(csvDir + CsvFiles.TAG_HAS_TYPE_TAGCLASS);
        defined.add(csvDir + CsvFiles.TAGCLASS_IS_SUBCLASS_OF_TAGCLASS);
        defined.add(csvDir + CsvFiles.ORGANISATION_IS_LOCATED_IN_PLACE);
        defined.add(csvDir + CsvFiles.PERSON_LIKES_COMMENT);
        defined.add(csvDir + CsvFiles.COMMENT_HAS_TAG_TAG);

        /*
         * Update Stream
         */
        defined.add(csvDir + CsvFiles.UPDATE_STREAM);

        // When

        // Then
        int prefixLength = Config.DATA_DIR.length();

        HashSet<String> definedMinusFolder = Sets.newHashSet(defined);
        definedMinusFolder.removeAll(CsvFiles.all(Config.DATA_DIR));

        HashSet<String> folderMinusDefined = (HashSet<String>) CsvFiles.all(Config.DATA_DIR);
        folderMinusDefined.removeAll(defined);

        assertThat(String.format("Files not found in folder: %s", removePrefixes(Lists.newArrayList(definedMinusFolder), prefixLength)), definedMinusFolder, equalTo(Sets.<String>newHashSet()));
        assertThat(String.format("Unexpected files in folder: %s", removePrefixes(Lists.newArrayList(folderMinusDefined), prefixLength)), folderMinusDefined, equalTo(Sets.<String>newHashSet()));
    }

    List<String> removePrefixes(List<String> original, int prefixLength) {
        List<String> withoutPrefix = new ArrayList<>();
        for (String string : original) {
            withoutPrefix.add(string.substring(prefixLength, string.length()));
        }
        return withoutPrefix;
    }
}
