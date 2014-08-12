package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery6<CONNECTION> implements Neo4jQuery<LdbcQuery6, LdbcQuery6Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer TAG_NAME = 2;
    protected static final Integer LIMIT = 3;

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "*1..2]-(:" + Domain.Nodes.Person + ")"
            + "<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")-[:" + Domain.Rels.HAS_TAG + "]->(:" + Domain.Nodes.Tag + " {" + Domain.Tag.NAME + ":{" + TAG_NAME + "}})\n"
            + "WITH DISTINCT post\n"
            + "MATCH (post)-[:" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")\n"
            + "WHERE NOT(tag." + Domain.Tag.NAME + "={" + TAG_NAME + "})\n"
            + "RETURN tag." + Domain.Tag.NAME + " AS tagName, count(tag) AS tagCount\n"
            + "ORDER BY tagCount DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
