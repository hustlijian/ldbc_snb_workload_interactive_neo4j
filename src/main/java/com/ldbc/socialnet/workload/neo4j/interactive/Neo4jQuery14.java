package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.Domain;

public abstract class Neo4jQuery14<CONNECTION> implements Neo4jQuery<LdbcQuery14, LdbcQuery14Result, CONNECTION> {
    protected static final Integer PERSON_ID_1 = 1;
    protected static final Integer PERSON_ID_2 = 2;
    protected static final Integer LIMIT = 3;

    protected static final String QUERY_STRING = ""
            + "MATCH path = (person1:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID_1 + "}})<-[:" + Domain.Rels.HAS_CREATOR + "]-()-[r:" + Domain.Rels.REPLY_OF + "*0..]-()"
            + "-[:" + Domain.Rels.HAS_CREATOR + "]->(person2:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{" + PERSON_ID_2 + "}})\n"
            + "WHERE all(message IN [n IN nodes(path) WHERE not(n:" + Domain.Nodes.Person + ")] WHERE (message)-[:" + Domain.Rels.HAS_CREATOR + "]->(person1) OR (message)-[:" + Domain.Rels.HAS_CREATOR + "]->(person2))\n"
            + "RETURN\n"
            + " [n IN nodes(path) | [labels(n)[0], n.id]] AS pathNodes,\n"
            + " reduce(weight = -0.5, n IN nodes(path) | \n"
            + "   CASE labels(n)[0]\n"
            + "     WHEN '" + Domain.Nodes.Post + "' THEN weight + 1.0\n"
            + "     WHEN '" + Domain.Nodes.Comment + "' THEN weight + 0.5\n"
            + "     ELSE weight\n"
            + "   END) AS weight\n"
            + "ORDER BY length(pathNodes) ASC, weight DESC\n"
            + "LIMIT {" + LIMIT + "}";
}
