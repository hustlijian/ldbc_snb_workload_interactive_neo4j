package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery4<CONNECTION> implements Neo4jQuery<LdbcQuery4, LdbcQuery4Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer MIN_DATE = 2;
    protected static final Integer MAX_DATE = 3;
    protected static final Integer LIMIT = 4;

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")"
            + "-[" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")\n"
            + "WHERE post." + Domain.Post.CREATION_DATE + " >= {" + MIN_DATE + "} AND post." + Domain.Post.CREATION_DATE + " <= {" + MAX_DATE + "}\n"
            + "WITH DISTINCT tag, collect(tag) AS tags\n"
            + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(tags) AS tagCount\n"
            + "ORDER BY tagCount DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
