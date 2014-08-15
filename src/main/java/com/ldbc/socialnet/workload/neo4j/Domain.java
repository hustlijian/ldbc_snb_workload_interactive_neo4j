package com.ldbc.socialnet.workload.neo4j;

import com.ldbc.driver.util.Tuple.Tuple2;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Domain {
    public static Iterable<Tuple2<Label, String>> labelPropertyPairsToIndex() {
        List<Tuple2<Label, String>> labelPropertyPairsToIndex = new ArrayList<>();
        labelPropertyPairsToIndex.add(new Tuple2<Label, String>(Nodes.Tag, Tag.NAME));
        labelPropertyPairsToIndex.add(new Tuple2<Label, String>(Nodes.Person, Person.ID));
        labelPropertyPairsToIndex.add(new Tuple2<Label, String>(Nodes.Person, Person.FIRST_NAME));
        labelPropertyPairsToIndex.add(new Tuple2<Label, String>(Nodes.Person, Person.LAST_NAME));
        labelPropertyPairsToIndex.add(new Tuple2<Label, String>(Place.Type.City, Place.NAME));
        labelPropertyPairsToIndex.add(new Tuple2<Label, String>(Place.Type.Country, Place.NAME));
        return labelPropertyPairsToIndex;
    }

    public static Set<Label> labelsToIndex() {
        Set<Label> labelsToIndex = new HashSet<>();
        for (Tuple2<Label, String> labelPropertyPair : labelPropertyPairsToIndex()) {
            labelsToIndex.add(labelPropertyPair._1());
        }
        return labelsToIndex;
    }

    public enum Rels implements RelationshipType {
        STUDY_AT,
        REPLY_OF,
        IS_LOCATED_IN,
        IS_PART_OF,
        KNOWS,
        HAS_MODERATOR,
        HAS_CREATOR,
        WORKS_AT,
        HAS_INTEREST,
        LIKES,
        HAS_MEMBER,
        CONTAINER_OF,
        HAS_TAG,
        HAS_TYPE,
        IS_SUBCLASS_OF
    }

    public enum Nodes implements Label {
        Comment,
        Post,
        Person,
        Forum,
        Tag,
        TagClass
    }

    /*
     * Nodes
     */

    public static class Comment {
        public static final String ID = "id";
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String CONTENT = "content";
    }

    public static class Post {
        public static final String ID = "id";
        public static final String IMAGE_FILE = "imageFile";
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String LANGUAGE = "language";
        public static final String CONTENT = "content";
    }

    public static class Person {
        public static final String ID = "id";
        public static final String FIRST_NAME = "firstName";
        public static final String LAST_NAME = "lastName";
        public static final String GENDER = "gender";
        public static final String BIRTHDAY = "birthday";
        public static final String BIRTHDAY_MONTH = "birthday_month";
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String LANGUAGES = "languages";
        public static final String EMAIL_ADDRESSES = "email";
    }

    public static class Forum {
        public static final String TITLE = "title";
        public static final String CREATION_DATE = "creationDate";
    }

    public static class Tag {
        public static final String NAME = "name";
        public static final String URI = "uri";
    }

    public static class TagClass {
        public static final String NAME = "name";
        public static final String URI = "uri";
    }

    public static class Organisation {
        public enum Type implements Label {
            University,
            Company
        }

        public static final String NAME = "name";
    }

    public static class Place {
        public enum Type implements Label {
            Country,
            City,
            Continent
        }

        public static final String NAME = "name";
        public static final String URI = "uri";
    }

    /*
     * Relationships
     */

    public static class StudiesAt {
        public static final String CLASS_YEAR = "classYear";
    }

    public static class WorksAt {
        public static final String WORK_FROM = "workFrom";
    }

    public static class Likes {
        public static final String CREATION_DATE = "creationDate";
    }

    public static class HasMember {
        public static final String JOIN_DATE = "joinDate";
    }

}
