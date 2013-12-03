package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery6Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery6;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class Neo4jQuery6EmbeddedCypher implements Neo4jQuery6
{
    @Override
    public String description()
    {
        return query();
    }

    @Override
    public Iterator<LdbcQuery6Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery6 operation )
    {
        return Iterators.transform( engine.execute( query(), buildParams( operation ) ).iterator(),
                new Function<Map<String, Object>, LdbcQuery6Result>()
                {
                    @Override
                    public LdbcQuery6Result apply( Map<String, Object> next )
                    {
                        return new LdbcQuery6Result( (String) next.get( "tagName" ), (long) next.get( "tagCount" ) );
                    }
                } );
    }

    private Map<String, Object> buildParams( LdbcQuery6 operation )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_id", operation.personId() );
        queryParams.put( "tag_name", operation.tagName() );
        return queryParams;
    }

    private String query()
    {
        return "MATCH (person:" + Nodes.Person + " {" + Person.ID + ":{person_id}})-[:" + Rels.KNOWS + "*1..2]-(:"
               + Nodes.Person + ")<-[:" + Rels.HAS_CREATOR + "]-(post:" + Nodes.Post + "),\n"

               + "  (post)-[:" + Rels.HAS_TAG + "]->(:" + Nodes.Tag + " {" + Tag.NAME + ":{tag_name}})\n"

               + "WITH DISTINCT post\n"

               + "MATCH (post)-[:" + Rels.HAS_TAG + "]->(tag:" + Nodes.Tag + ")\n"

               + "WHERE NOT(tag." + Tag.NAME + "={tag_name})\n"

               + "RETURN tag." + Tag.NAME + " AS tagName, count(tag) AS tagCount\n"

               + "ORDER BY tagCount DESC\n"

               + "LIMIT 10";
    }
}
