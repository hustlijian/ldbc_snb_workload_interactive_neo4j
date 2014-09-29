package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.snb.interactive.neo4j.interactive.remote_cypher.*;

public class Neo4jDbCommandsJdbcRemoteCypher extends Neo4jDbCommands {
    private final String url;
    private Neo4jConnectionState dbConnectionState;

    public Neo4jDbCommandsJdbcRemoteCypher(String url) {
        this.url = url;
    }

    @Override
    public void init() throws DbException {
        dbConnectionState = new Neo4jConnectionState(null, null, null, url);
    }

    @Override
    public void cleanUp() throws DbException {
        dbConnectionState.shutdown();
    }

    @Override
    public DbConnectionState getDbConnectionState() {
        return dbConnectionState;
    }

    @Override
    public void registerHandlersWithDb(Db db) throws DbException {
        db.registerOperationHandler(LdbcQuery1.class, LdbcQuery1HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery2.class, LdbcQuery2HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery3.class, LdbcQuery3HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery4.class, LdbcQuery4HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery5.class, LdbcQuery5HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery6.class, LdbcQuery6HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery7.class, LdbcQuery7HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery8.class, LdbcQuery8HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery9.class, LdbcQuery9HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery10.class, LdbcQuery10HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery11.class, LdbcQuery11HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery12.class, LdbcQuery12HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery13.class, LdbcQuery13HandlerRemoteCypher.class);
        db.registerOperationHandler(LdbcQuery14.class, LdbcQuery14HandlerRemoteCypher.class);
    }
}
