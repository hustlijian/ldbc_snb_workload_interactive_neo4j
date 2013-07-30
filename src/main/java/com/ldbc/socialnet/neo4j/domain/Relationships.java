package com.ldbc.socialnet.neo4j.domain;

import org.neo4j.graphdb.RelationshipType;

public enum Relationships implements RelationshipType
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
    LIKE,
    HAS_MEMBER,
    CONTAINER_OF,
    HAS_TAG,
    HAS_TYPE,
    HAS_SUBCLASS_OF
}
