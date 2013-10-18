package com.ldbc.socialnet.neo4j.workload;

import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery1;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery3;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery4;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery5;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery6;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery7;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api.Neo4jQuery1EmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api.Neo4jQuery3EmbeddedApi;
import com.ldbc.socialnet.workload.neo4j.transaction.embedded_api.Neo4jQuery4EmbeddedApi;

public class QueryCorrectnessEmbeddedApiTest extends QueryCorrectnessTest
{

    @Override
    public Neo4jQuery1 neo4jQuery1Impl()
    {
        return new Neo4jQuery1EmbeddedApi();
    }

    @Override
    public Neo4jQuery3 neo4jQuery3Impl()
    {
        return new Neo4jQuery3EmbeddedApi();
    }

    @Override
    public Neo4jQuery4 neo4jQuery4Impl()
    {
        return new Neo4jQuery4EmbeddedApi();
    }

    @Override
    public Neo4jQuery5 neo4jQuery5Impl()
    {
        return null;
    }

    @Override
    public Neo4jQuery6 neo4jQuery6Impl()
    {
        return null;
    }

    @Override
    public Neo4jQuery7 neo4jQuery7Impl()
    {
        return null;
    }

}
