package com.ldbc.socialnet.workload.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.embedded_cypher.*;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.IOException;
import java.util.Map;

public class Neo4jDbCommandsJdbcRemoteCypher extends Neo4jDbCommands {
    final private String dbPath;
    final private String configPath;
    private ExecutionEngine queryEngine;
    private GraphDatabaseService db;
    private DbConnectionState dbConnectionState;

    public Neo4jDbCommandsJdbcRemoteCypher(String dbPath, String configPath) {
        this.dbPath = dbPath;
        this.configPath = configPath;
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
        queryEngine = new ExecutionEngine(db);
        dbConnectionState = new Neo4jConnectionStateEmbedded(db, queryEngine, null);
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
        db.registerOperationHandler(LdbcQuery1.class, LdbcQuery1HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery2.class, LdbcQuery2HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery3.class, LdbcQuery3HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery4.class, LdbcQuery4HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery5.class, LdbcQuery5HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery6.class, LdbcQuery6HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery7.class, LdbcQuery7HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery8.class, LdbcQuery8HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery9.class, LdbcQuery9HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery10.class, LdbcQuery10HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery11.class, LdbcQuery11HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery12.class, LdbcQuery12HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery13.class, LdbcQuery13HandlerEmbeddedCypher.class);
        db.registerOperationHandler(LdbcQuery14.class, LdbcQuery14HandlerEmbeddedCypher.class);
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
