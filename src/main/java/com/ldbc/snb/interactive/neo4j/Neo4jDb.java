package com.ldbc.snb.interactive.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.util.MapUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Map;

/**
 * Neo4j client for LDBC DShini workload
 * <p/>
 * Properties to set:
 * <p/>
 * neo4j.url=http://localhost:7474/db/data <br>
 * neo4j.clear=false <br>
 * neo4j.path=/tmp/db <br>
 * neo4j.dbtype=embedded <br>
 */

public class Neo4jDb extends Db {
    private static Logger logger = Logger.getLogger(Neo4jDb.class);

    public static String URL_KEY = "neo4j.url";

    public static String DB_PATH_KEY = "neo4j.path";

    public static String CONFIG_PATH_KEY = "neo4j.config";

    public static String DB_TYPE_KEY = "neo4j.dbtype";
    public static String DB_TYPE_VALUE_REMOTE_CYPHER = "remote-cypher";
    public static String DB_TYPE_VALUE_EMBEDDED_CYPHER = "embedded-cypher";
    public static String DB_TYPE_VALUE_EMBEDDED_API = "embedded-api-steps";

    private String url;
    private String dbType;
    private String dbPath;
    private String configPath;

    private Neo4jDbCommands commands;

    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
        // Initialize Neo4j driver
        url = MapUtils.getDefault(properties, URL_KEY, "http://localhost:7474/db/data");
        dbPath = MapUtils.getDefault(properties, DB_PATH_KEY, "/tmp/db");
        configPath = MapUtils.getDefault(properties, CONFIG_PATH_KEY, "/tmp/config.properties");
        dbType = MapUtils.getDefault(properties, DB_TYPE_KEY, "UNDEFINED");

        logger.info("*** Neo4j Properties ***");
        logger.info("database type = " + dbType);
        logger.info("url = " + url);
        logger.info("db path = " + new File(dbPath).getAbsolutePath());
        logger.info("config path = " + new File(configPath).getAbsolutePath());
        logger.info("************************");

        if (dbType.equals(DB_TYPE_VALUE_REMOTE_CYPHER)) {
            logger.info("Connecting to database: " + url);
            logger.info("API type: JDBC");
            commands = new Neo4jDbCommandsJdbcCypher(url);
        } else if (dbType.equals(DB_TYPE_VALUE_EMBEDDED_CYPHER)) {
            logger.info("Connecting to database: " + dbPath);
            logger.info("API type: Cypher");
            commands = new Neo4jDbCommandsEmbeddedCypher(dbPath, configPath);
        } else if (dbType.equals(DB_TYPE_VALUE_EMBEDDED_API)) {
            logger.info("Connecting to database: " + dbPath);
            logger.info("API type: Traversal Framework - " + Neo4jDbCommandsEmbeddedApi.LdbcTraversersType.STEPS.name());
            commands = new Neo4jDbCommandsEmbeddedApi(dbPath, configPath, Neo4jDbCommandsEmbeddedApi.LdbcTraversersType.STEPS);
        } else {
            throw new DbException(String.format("Invalid database type: %s", dbType));
        }

        commands.init();
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
