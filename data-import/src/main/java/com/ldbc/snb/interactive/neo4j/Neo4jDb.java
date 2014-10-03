package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Map;

public class Neo4jDb extends Db {
    private static Logger logger = Logger.getLogger(Neo4jDb.class);

    public static String URL_KEY = "neo4j.url";
    public static String DB_PATH_KEY = "neo4j.path";
    public static String CONFIG_PATH_KEY = "neo4j.config";
    public static String DB_TYPE_KEY = "neo4j.dbtype";
    public static String DB_TYPE_VALUE_REMOTE_CYPHER = "remote-cypher";
    public static String DB_TYPE_VALUE_EMBEDDED_CYPHER = "embedded-cypher";
    public static String DB_TYPE_VALUE_EMBEDDED_API = "embedded-api-steps";
    public static String WARMUP_KEY = "neo4j.warmup";

    private Neo4jDbCommands commands;

    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
        // Initialize Neo4j driver
        String url = properties.get(URL_KEY);
        String dbPath = properties.get(DB_PATH_KEY);
        String configPath = properties.get(CONFIG_PATH_KEY);
        String dbType = properties.get(DB_TYPE_KEY);
        String doWarmupString = (null == properties.get(WARMUP_KEY)) ? "false" : properties.get(WARMUP_KEY);

        logger.info("*** Neo4j Properties ***");
        logger.info("database type = " + ((null == dbType) ? "UNKNOWN" : dbType));
        logger.info("url = " + ((null == url) ? "UNKNOWN" : url));
        logger.info("db path = " + ((null == dbPath) ? "UNKNOWN" : new File(dbPath).getAbsolutePath()));
        logger.info("config path = " + ((null == configPath) ? "UNKNOWN" : new File(configPath).getAbsolutePath()));
        logger.info("warmup = " + doWarmupString);
        logger.info("************************");

        if (null == dbType) throw new DbException("Neo4j database connector type not given");
        boolean doWarmup = (null == doWarmupString) ? false : Boolean.parseBoolean(doWarmupString);

        if (dbType.equals(DB_TYPE_VALUE_REMOTE_CYPHER)) {
            logger.info("Connecting to database: " + url);
            logger.info("API type: JDBC");
            if (null == url) throw new DbException("Neo4j server URL not given");
            commands = new Neo4jDbCommandsJdbcRemoteCypher(url);
        } else if (dbType.equals(DB_TYPE_VALUE_EMBEDDED_CYPHER)) {
            logger.info("Connecting to database: " + dbPath);
            logger.info("API type: Cypher");
            if (null == dbPath) throw new DbException("Neo4j database path not given");
            if (null == configPath) throw new DbException("Neo4j database configuration not given");
            commands = new Neo4jDbCommandsEmbeddedCypher(dbPath, configPath);
        } else if (dbType.equals(DB_TYPE_VALUE_EMBEDDED_API)) {
            logger.info("Connecting to database: " + dbPath);
            logger.info("API type: Traversal Framework - " + Neo4jDbCommandsEmbeddedApi.LdbcTraversersType.STEPS.name());
            if (null == dbPath) throw new DbException("Neo4j database path not given");
            if (null == configPath) throw new DbException("Neo4j database configuration not given");
            commands = new Neo4jDbCommandsEmbeddedApi(dbPath, configPath, Neo4jDbCommandsEmbeddedApi.LdbcTraversersType.STEPS);
        } else {
            throw new DbException(String.format("Invalid database type: %s", dbType));
        }

        commands.init(doWarmup);
        commands.registerHandlersWithDb(this);
        logger.info("Initialization complete");
    }

    @Override
    protected void onCleanup() throws DbException {
        try {
            commands.cleanUp();
        } catch (Exception e) {
            throw new DbException("Error encountered during cleanup", e);
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return commands.getDbConnectionState();
    }
}
