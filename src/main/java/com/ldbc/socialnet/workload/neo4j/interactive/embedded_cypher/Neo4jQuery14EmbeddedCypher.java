package com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery14;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;

public class Neo4jQuery14EmbeddedCypher implements Neo4jQuery14 {
    /*
        Description
            Find all paths between two specified persons, where paths may be comprised of: Person, Comment, and Post entities; Knows, ReplyOf, and HasCreator relationships.
            Calculate the weight of the paths given the following rules: each reply to a post contributes 1.0 to the weight, each reply to a comment contributes 0.5 to the weight.
            The weight should be symmetrical.
            The result should be sorted descending by weight, and only top 10 paths should be returned.
        Parameter
            Person1.id
            Person2.id
        Result
            path - ordered sequence of IDs, alternating between entity IDs and relationship IDs, and starting & ending with the IDs of the connected persons
            weight
    */
    private static final String QUERY_STRING = ""
            + "MATCH path = (person1:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id_1}})<-[:" + Domain.Rels.HAS_CREATOR + "]-()-[r:" + Domain.Rels.REPLY_OF + "*0..]-()-[:" + Domain.Rels.HAS_CREATOR + "]->(person2:" + Domain.Nodes.Person + " {" + Domain.Person.ID + ":{person_id_2}})\n"
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
            + "LIMIT {limit}";

    @Override
    public String description() {
        return QUERY_STRING;
    }

    @Override
    public Iterator<LdbcQuery14Result> execute(GraphDatabaseService db, ExecutionEngine engine, LdbcQuery14 operation) {
        return Iterators.transform(engine.execute(QUERY_STRING, buildParams(operation)).iterator(),
                new Function<Map<String, Object>, LdbcQuery14Result>() {
                    @Override
                    public LdbcQuery14Result apply(Map<String, Object> row) {
                        Collection<LdbcQuery14Result.PathNode> pathNodes = Collections2.transform((Collection<Collection<Object>>) row.get("pathNodes"), new Function<Collection<Object>, LdbcQuery14Result.PathNode>() {
                            @Override
                            public LdbcQuery14Result.PathNode apply(Collection<Object> pathNode) {
                                List<Object> pathNodeList = Lists.newArrayList(pathNode);
                                return new LdbcQuery14Result.PathNode((String) pathNodeList.get(0), (long) pathNodeList.get(1));
                            }
                        });
                        return new LdbcQuery14Result(pathNodes, (double) row.get("weight"));
                    }
                });
    }

    private Map<String, Object> buildParams(LdbcQuery14 operation) {
        Map<String, Object> queryParams = new HashMap<>();
        queryParams.put("person_id_1", operation.personId1());
        queryParams.put("person_id_2", operation.personId2());
        queryParams.put("limit", operation.limit());
        return queryParams;
    }
}
