package com.ldbc.socialnet.neo4j.workload;

import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery1;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery2;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery3;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery4;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery5;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery6;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery7;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery1EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery2EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery3EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery4EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery5EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery6EmbeddedCypher;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher.Neo4jQuery7EmbeddedCypher;

public class QueryCorrectnessEmbeddedCypherTest extends QueryCorrectnessTest
{

    @Override
    public Neo4jQuery1 neo4jQuery1Impl()
    {
        return new Neo4jQuery1EmbeddedCypher();
    }

    @Override
    public Neo4jQuery2 neo4jQuery2Impl()
    {
        return new Neo4jQuery2EmbeddedCypher();
    }

    @Override
    public Neo4jQuery3 neo4jQuery3Impl()
    {
        return new Neo4jQuery3EmbeddedCypher();
    }

    @Override
    public Neo4jQuery4 neo4jQuery4Impl()
    {
        return new Neo4jQuery4EmbeddedCypher();
    }

    @Override
    public Neo4jQuery5 neo4jQuery5Impl()
    {
        return new Neo4jQuery5EmbeddedCypher();
    }

    @Override
    public Neo4jQuery6 neo4jQuery6Impl()
    {
        return new Neo4jQuery6EmbeddedCypher();
    }

    @Override
    public Neo4jQuery7 neo4jQuery7Impl()
    {
        return new Neo4jQuery7EmbeddedCypher();
    }

    @Override
    public Object neo4jQuery8Impl()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object neo4jQuery9Impl()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object neo4jQuery10Impl()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object neo4jQuery11Impl()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object neo4jQuery12Impl()
    {
        // TODO Auto-generated method stub
        return null;
    }

}
