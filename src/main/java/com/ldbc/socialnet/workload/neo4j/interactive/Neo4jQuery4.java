package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery4<CONNECTION> implements Neo4jQuery<LdbcQuery4, LdbcQuery4Result, CONNECTION> {
    protected static final Integer PERSON_ID = 1;
    protected static final Integer MIN_DATE = 2;
    protected static final Integer MAX_DATE = 3;
    protected static final Integer LIMIT = 4;

    /*
    Given a start Person, find Tags that are attached to Posts that were created by that Person's friends.
    Only include Tags that were attached to Posts created within a given time interval, and that were never attached to Posts created before this interval.
    Return top 10 Tags, and the count of Posts, which were created within the given time interval, that this Tag was attached to.
    Sort results descending by Post count, and then ascending by Tag name.
     */

    // TODO also correct, and less confusing, but maybe less efficient because the WITH might materialize things prematurely
//    protected static final String QUERY_STRING = ""
//            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(friend:" + Domain.Nodes.Person + ")"
//            + "<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")-[" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")\n"
//            + "WHERE post." + Domain.Message.CREATION_DATE + " >= {" + MIN_DATE + "} AND post." + Domain.Message.CREATION_DATE + " < {" + MAX_DATE + "}\n"
//            + "WITH tag, length(collect(post)) AS postCount\n"
//            + "OPTIONAL MATCH (tag)<-[:" + Domain.Rels.HAS_TAG + "]-(oldPost:" + Domain.Nodes.Post + ")\n"
//            + "WHERE oldPost." + Domain.Message.CREATION_DATE + " < {" + MIN_DATE + "}\n"
//            + "WITH tag, postCount, length(collect(oldPost)) AS oldPostCount\n"
//            + "WHERE oldPostCount=0\n"
//            + "RETURN tag." + Domain.Tag.NAME + " AS tagName, postCount\n"
//            + "ORDER BY postCount DESC, tagName ASC\n"
//            + "LIMIT {" + LIMIT + "}";

    protected static final String QUERY_STRING = ""
            + "MATCH (person:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID + "}})-[:" + Domain.Rels.KNOWS + "]-(:" + Domain.Nodes.Person + ")"
            + "<-[:" + Domain.Rels.HAS_CREATOR + "]-(post:" + Domain.Nodes.Post + ")-[" + Domain.Rels.HAS_TAG + "]->(tag:" + Domain.Nodes.Tag + ")\n"
            + "WHERE post." + Domain.Message.CREATION_DATE + " >= {" + MIN_DATE + "} AND post." + Domain.Message.CREATION_DATE + " < {" + MAX_DATE + "}\n"
            + "OPTIONAL MATCH (tag)<-[:" + Domain.Rels.HAS_TAG + "]-(oldPost:" + Domain.Nodes.Post + ")\n"
            + "WHERE oldPost." + Domain.Message.CREATION_DATE + " < {" + MIN_DATE + "}\n"
            + "WITH tag, post, length(collect(oldPost)) AS oldPostCount\n"
            + "WHERE oldPostCount=0\n"
            + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(collect(post)) AS postCount\n"
            + "ORDER BY postCount DESC, tagName ASC\n"
            + "LIMIT {" + LIMIT + "}";
}
