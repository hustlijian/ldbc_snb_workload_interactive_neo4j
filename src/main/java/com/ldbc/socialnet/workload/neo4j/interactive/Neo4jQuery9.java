package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery9<CONNECTION> implements Neo4jQuery<LdbcQuery9, LdbcQuery9Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer LATEST_DATE = 2;
    protected static final Integer LIMIT = 3;

    protected static final String QUERY_STRING = ""
            + "MATCH (:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "*1..2]-(friend:" + Domain.Nodes.Person + ")\n"
            + "MATCH (friend)<-[:" + Domain.Rels.HAS_CREATOR + "]-(activity) WHERE activity." + Domain.Message.CREATION_DATE + " < {" + LATEST_DATE + "}\n"
            + "RETURN DISTINCT"
            + " activity." + Domain.Message.ID + " AS activityId,"
            + " activity." + Domain.Message.CONTENT + " AS activityContent,"
            + " activity." + Domain.Message.CREATION_DATE + " AS activityCreationDate,"
            + " friend." + Domain.Person.ID + " AS personId,"
            + " friend." + Domain.Person.FIRST_NAME + " AS personFirstName,"
            + " friend." + Domain.Person.LAST_NAME + " AS personLastName\n"
            + "ORDER BY activity.creationDate DESC, activity.id ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
