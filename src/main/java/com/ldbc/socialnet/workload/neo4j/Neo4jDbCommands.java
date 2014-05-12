package com.ldbc.socialnet.workload.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;

public abstract class Neo4jDbCommands {
    public abstract void init() throws DbException;

    public abstract void cleanUp() throws DbException;

    public abstract DbConnectionState getDbConnectionState() throws DbException;

    public abstract void registerHandlersWithDb(Db db) throws DbException;
}
