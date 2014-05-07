package com.ldbc.socialnet.neo4j.workload;

import com.ldbc.socialnet.workload.neo4j.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher.*;

public class QueryCorrectnessEmbeddedCypherTest extends QueryCorrectnessTest {

    @Override
    public Neo4jQuery1 neo4jQuery1Impl() {
        return new Neo4jQuery1EmbeddedCypher();
    }

    @Override
    public Neo4jQuery2 neo4jQuery2Impl() {
        return new Neo4jQuery2EmbeddedCypher();
    }

    @Override
    public Neo4jQuery3 neo4jQuery3Impl() {
        return new Neo4jQuery3EmbeddedCypher();
    }

    @Override
    public Neo4jQuery4 neo4jQuery4Impl() {
        return new Neo4jQuery4EmbeddedCypher();
    }

    @Override
    public Neo4jQuery5 neo4jQuery5Impl() {
        return new Neo4jQuery5EmbeddedCypher();
    }

    @Override
    public Neo4jQuery6 neo4jQuery6Impl() {
        return new Neo4jQuery6EmbeddedCypher();
    }

    @Override
    public Neo4jQuery7 neo4jQuery7Impl() {
        return new Neo4jQuery7EmbeddedCypher();
    }

    @Override
    public Neo4jQuery8 neo4jQuery8Impl() {
        return new Neo4jQuery8EmbeddedCypher();
    }

    @Override
    public Neo4jQuery9 neo4jQuery9Impl() {
        return new Neo4jQuery9EmbeddedCypher();
    }

    @Override
    public Neo4jQuery10 neo4jQuery10Impl() {
        return new Neo4jQuery10EmbeddedCypher();
    }

    @Override
    public Neo4jQuery11 neo4jQuery11Impl() {
        return new Neo4jQuery11EmbeddedCypher();
    }

    @Override
    public Object neo4jQuery12Impl() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object neo4jQuery13Impl() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object neo4jQuery14Impl() {
        // TODO Auto-generated method stub
        return null;
    }
}