package com.ldbc.socialnet.neo4j.workload;

import com.ldbc.socialnet.workload.neo4j.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps.*;
import org.junit.Ignore;

@Ignore
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
        return new Neo4jQuery8EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery9 neo4jQuery9Impl() {
        return new Neo4jQuery9EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery10 neo4jQuery10Impl() {
        return new Neo4jQuery10EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery11 neo4jQuery11Impl() {
        return new Neo4jQuery11EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery12 neo4jQuery12Impl() {
        return new Neo4jQuery12EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery13 neo4jQuery13Impl() {
        return new Neo4jQuery13EmbeddedApi(new LdbcTraversersSteps(db));
    }

    @Override
    public Neo4jQuery14 neo4jQuery14Impl() {
        return new Neo4jQuery14EmbeddedApi(new LdbcTraversersSteps(db));
    }
}
