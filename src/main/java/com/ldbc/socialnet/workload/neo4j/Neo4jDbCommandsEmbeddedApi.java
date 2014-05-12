package com.ldbc.socialnet.workload.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps.*;
import com.ldbc.socialnet.workload.neo4j.utils.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4jDbCommandsEmbeddedApi extends Neo4jDbCommands {
    private final String path;
    private final LdbcTraversersType traversersType;
    private GraphDatabaseService db;
    private DbConnectionState dbConnectionState;
    private LdbcTraversers traversers;

    public enum LdbcTraversersType {
        RAW,
        STEPS
    }

    public Neo4jDbCommandsEmbeddedApi(String path, LdbcTraversersType traversersType) {
        this.path = path;
        this.traversersType = traversersType;
    }

    @Override
    public void init() {
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(path).setConfig(Config.NEO4J_RUN_CONFIG).newGraphDatabase();
        switch (traversersType) {
            case RAW:
                traversers = new LdbcTraversersRaw(db);
                break;
            case STEPS:
                traversers = new LdbcTraversersSteps(db);
                break;
            default:
                throw new RuntimeException("Unrecognized LdbcTraversersType: " + traversersType.name());
        }
        dbConnectionState = new Neo4jConnectionStateEmbedded(db, null, traversers);
        registerShutdownHook(db);
    }

    @Override
    public void cleanUp() {
        db.shutdown();
    }

    @Override
    public DbConnectionState getDbConnectionState() {
        return dbConnectionState;
    }

    @Override
    public void registerHandlersWithDb(Db db) throws DbException {
        db.registerOperationHandler(LdbcQuery1.class, LdbcQuery1HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery2.class, LdbcQuery2HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery3.class, LdbcQuery3HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery4.class, LdbcQuery4HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery5.class, LdbcQuery5HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery6.class, LdbcQuery6HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery7.class, LdbcQuery7HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery8.class, LdbcQuery8HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery9.class, LdbcQuery9HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery10.class, LdbcQuery10HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery11.class, LdbcQuery11HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery12.class, LdbcQuery12HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery13.class, LdbcQuery13HandlerEmbeddedApi.class);
        db.registerOperationHandler(LdbcQuery14.class, LdbcQuery14HandlerEmbeddedApi.class);
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }
}
