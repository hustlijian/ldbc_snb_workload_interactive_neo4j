package com.ldbc.socialnet.workload.neo4j;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.socialnet.workload.neo4j.Neo4jDbCommandsEmbeddedApi.LdbcTraversersType;
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
    public static String PATH_KEY = "neo4j.path";
    public static String DB_TYPE_KEY = "neo4j.dbtype";
    public static String DB_TYPE_VALUE_REMOTE_CYPHER = "remote-cypher";
    public static String DB_TYPE_VALUE_EMBEDDED_CYPHER = "embedded-cypher";
    public static String DB_TYPE_VALUE_EMBEDDED_STEPS = "embedded-api-steps";
    public static String DB_TYPE_VALUE_EMBEDDED_API = "embedded-api-raw";

    private String url;
    private String dbType;
    private String path;

    private Neo4jDbCommands commands;

    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
        // Initialize Neo4j driver
        url = MapUtils.getDefault(properties, URL_KEY, "http://localhost:7474/db/data");
        path = MapUtils.getDefault(properties, PATH_KEY, "/tmp/db");
        dbType = MapUtils.getDefault(properties, DB_TYPE_KEY, "UNDEFINED");

        logger.info("*** Neo4j Properties ***");
        logger.info("database type = " + dbType);
        logger.info("url = " + url);
        logger.info("path = " + new File(path).getAbsolutePath());
        logger.info("************************");

        if (dbType.equals(DB_TYPE_VALUE_REMOTE_CYPHER)) {
            // logger.info( "Connecting to database: " + url );
            // TODO implement
            throw new DbException("Remote Cypher commands not implemented yet");
        } else if (dbType.equals(DB_TYPE_VALUE_EMBEDDED_CYPHER)) {
            logger.info("Connecting to database: " + path);
            logger.info("API type: Cypher");
            commands = new Neo4jDbCommandsEmbeddedCypher(path);
        } else if (dbType.equals(DB_TYPE_VALUE_EMBEDDED_STEPS)) {
            logger.info("Connecting to database: " + path);
            logger.info("API type: Traversal Framework - " + LdbcTraversersType.STEPS.name());
            commands = new Neo4jDbCommandsEmbeddedApi(path, LdbcTraversersType.STEPS);
        } else if (dbType.equals(DB_TYPE_VALUE_EMBEDDED_API)) {
            // logger.info( "Connecting to database: " + path );
            // logger.info( "API type: Traversal Framework - " +
            // LdbcTraversersType.RAW.name() );
            // commands = new Neo4jDbCommandsEmbeddedApi( path,
            // LdbcTraversersType.RAW );
            // TODO implement
            throw new DbException("Raw API commands not implemented yet");
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
            String msg = "Error encountered during cleanup";
            logger.error(msg, e.getCause());
            throw new DbException(msg, e.getCause());
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return commands.getDbConnectionState();
    }
}
