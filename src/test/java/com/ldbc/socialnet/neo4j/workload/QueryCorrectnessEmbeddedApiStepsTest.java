package com.ldbc.socialnet.neo4j.workload;

import com.ldbc.socialnet.workload.neo4j.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps.*;

public class QueryCorrectnessEmbeddedApiStepsTest extends QueryCorrectnessTest {

    @Override
    public Neo4jQuery1 neo4jQuery1Impl() {
        return new Neo4jQuery1EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery2 neo4jQuery2Impl() {
        return new Neo4jQuery2EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery3 neo4jQuery3Impl() {
        return new Neo4jQuery3EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery4 neo4jQuery4Impl() {
        return new Neo4jQuery4EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery5 neo4jQuery5Impl() {
        return new Neo4jQuery5EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery6 neo4jQuery6Impl() {
        return new Neo4jQuery6EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery7 neo4jQuery7Impl() {
        return new Neo4jQuery7EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery8 neo4jQuery8Impl() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Neo4jQuery9 neo4jQuery9Impl() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Neo4jQuery10 neo4jQuery10Impl() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Neo4jQuery11 neo4jQuery11Impl() {
        // TODO Auto-generated method stub
        return null;
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
