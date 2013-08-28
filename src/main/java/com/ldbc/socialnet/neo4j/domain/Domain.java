package com.ldbc.socialnet.neo4j.domain;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class Domain
{

    public enum Rel implements RelationshipType
    {
        STUDY_AT,
        REPLY_OF,
        IS_LOCATED_IN,
        IS_PART_OF,
        KNOWS,
        HAS_MODERATOR,
        HAS_CREATOR,
        WORKS_AT,
        HAS_INTEREST,
        HAS_EMAIL_ADDRESS,
        ANNOTATED_WITH,
        LIKES,
        HAS_MEMBER,
        CONTAINER_OF,
        HAS_TAG,
        HAS_TYPE,
        IS_SUBCLASS_OF
    }

    public enum Node implements Label
    {
        COMMENT,
        POST,
        PERSON,
        FORUM,
        TAG,
        TAG_CLASS,
        ORGANISATION,
        LANGUAGE,
        PLACE,
        EMAIL_ADDRESS
    }

    /*
     * Nodes
     */

    public static class Comment
    {
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String CONTENT = "content";
    }

    public static class Post
    {
        public static final String IMAGE_FILE = "imageFile";
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String LANGUAGE = "language";
        public static final String CONTENT = "content";
    }

    public static class Person
    {
        public static final String ID = "id";
        public static final String FIRST_NAME = "firstName";
        public static final String LAST_NAME = "lastName";
        public static final String GENDER = "gender";
        public static final String BIRTHDAY = "birthday";
        public static final String CREATION_DATE = "creationDate";
        public static final String LOCATION_IP = "locationIP";
        public static final String BROWSER_USED = "browserUsed";
        public static final String LANGUAGES = "language";
        public static final String EMAIL_ADDRESSES = "email";
    }

    public static class Forum
    {
        public static final String TITLE = "title";
        public static final String CREATION_DATE = "creationDate";
    }

    public static class Tag
    {
        public static final String NAME = "name";
        public static final String URL = "url";
    }

    public static class TagClass
    {
        public static final String NAME = "name";
        public static final String URL = "url";
    }

    public static class Organisation
    {
        public enum Type implements Label
        {
            UNIVERSITY,
            COMPANY
        }

        public static final String NAME = "name";
    }

    public static class Place
    {
        public enum Type implements Label
        {
            COUNTRY,
            CITY,
            CONTINENT
        }

        public static final String NAME = "name";
        public static final String URL = "url";
    }

    /*
     * Relationships
     */

    public static class StudiesAt
    {
        public static final String CLASS_YEAR = "classYear";
    }

    public static class WorksAt
    {
        public static final String WORK_FROM = "workFrom";
    }

    public static class Likes
    {
        public static final String CREATION_DATE = "creationDate";
    }

    public static class HasMember
    {
        public static final String JOIN_DATE = "joinDate";
    }
}
