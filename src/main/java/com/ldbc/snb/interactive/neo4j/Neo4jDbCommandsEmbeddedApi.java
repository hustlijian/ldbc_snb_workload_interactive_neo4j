package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps.*;
import com.ldbc.snb.interactive.neo4j.utils.Utils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.Map;

public class Neo4jDbCommandsEmbeddedApi extends Neo4jDbCommands {
    private final String dbPath;
    final private String configPath;
    private final LdbcTraversersType traversersType;
    private GraphDatabaseService db;
    private DbConnectionState dbConnectionState;
    private LdbcTraversers traversers;

    public enum LdbcTraversersType {
        STEPS
    }

    public Neo4jDbCommandsEmbeddedApi(String dbPath, String configPath, LdbcTraversersType traversersType) {
        this.dbPath = dbPath;
        this.configPath = configPath;
        this.traversersType = traversersType;
    }

    @Override
    public void init() throws DbException {
        Map dbConfig;
        try {
            dbConfig = Utils.loadConfig(configPath);
        } catch (IOException e) {
            throw new DbException("Unable to load Neo4j DB config", e);
        }
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbPath).setConfig(dbConfig).newGraphDatabase();
        switch (traversersType) {
            case STEPS:
                traversers = new LdbcTraversers(db);
                break;
            default:
                throw new RuntimeException("Unrecognized LdbcTraversersType: " + traversersType.name());
        }
        dbConnectionState = new Neo4jConnectionState(db, null, traversers, null);
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
