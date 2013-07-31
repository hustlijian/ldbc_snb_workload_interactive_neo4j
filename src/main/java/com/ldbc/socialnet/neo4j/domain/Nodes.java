package com.ldbc.socialnet.neo4j.domain;

import org.neo4j.graphdb.Label;

public enum Nodes implements Label
{
    Comment,
    Post,
    Person,
    Forum,
    Tag,
    TagClass,
    Organisation,
    Language,
    Location,
    EmailAddress
}
