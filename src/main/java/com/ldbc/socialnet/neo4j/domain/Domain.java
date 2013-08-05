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
        SPEAKS,
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

    public enum PlaceType implements Label
    {
        COUNTRY,
        CITY,
        CONTINENT
    }

    public enum OrganisationType implements Label
    {
        UNIVERSITY,
        COMPANY
    }
}
