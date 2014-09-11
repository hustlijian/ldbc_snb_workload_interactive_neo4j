package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;

import static com.ldbc.snb.interactive.neo4j.Domain.*;

public abstract class Neo4jQuery14<CONNECTION> implements Neo4jQuery<LdbcQuery14, LdbcQuery14Result, CONNECTION> {
    protected static final Integer PERSON_ID_1 = 1;
    protected static final Integer PERSON_ID_2 = 2;

    /*
    Given two Persons, find all weighted paths of the shortest length between these two Persons in the sub-graph induced by the Knows relationship.
    The nodes in the path are Persons.
    Weight of a path is sum of weights between every pair of consecutive Person nodes in the path.
    The weight for a pair of Persons is calculated such that every reply (by one of the Persons) to a Post (by the other Person) contributes 1.0,
    and every reply (by one of the Persons) to a Comment (by the other Person) contributes 0.5.
    Return all the paths with shortest length, and their weights.
    Sort results descending by path weight.
     */
    protected static final String QUERY_STRING = ""
            + "MATCH path = allShortestPaths((person1:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID_1 + "}})-[:" + Rels.KNOWS + "]-(person2:" + Nodes.Person + " {" + Person.ID + ":{" + PERSON_ID_2 + "}}))\n"
            + "WITH nodes(path) AS pathNodes\n"
            + "RETURN\n"
            + " extract(n IN pathNodes | n.id) AS pathNodeIds,\n"
            + " reduce(weight=0.0, idx IN range(1,size(pathNodes)-1) |\n"
            + "    extract(prev IN [pathNodes[idx-1]] |\n"
            + "        extract(curr IN [pathNodes[idx]] |\n"
            + "            weight +\n"
            + "            length((curr)<-[:" + Rels.HAS_CREATOR + "]-(:" + Nodes.Comment + ")-[:" + Rels.REPLY_OF + "]->(:" + Nodes.Post + ")-[:" + Rels.HAS_CREATOR + "]->(prev))*1.0 +\n"
            + "            length((prev)<-[:" + Rels.HAS_CREATOR + "]-(:" + Nodes.Comment + ")-[:" + Rels.REPLY_OF + "]->(:" + Nodes.Post + ")-[:" + Rels.HAS_CREATOR + "]->(curr))*1.0 +\n"
            + "            length((prev)-[:" + Rels.HAS_CREATOR + "]-(:" + Nodes.Comment + ")-[:" + Rels.REPLY_OF + "]-(:" + Nodes.Comment + ")-[:" + Rels.HAS_CREATOR + "]-(curr))*0.5\n"
            + "        )\n"
            + "    )[0][0]\n"
            + " ) AS weight\n"
            + "ORDER BY weight DESC";
}
