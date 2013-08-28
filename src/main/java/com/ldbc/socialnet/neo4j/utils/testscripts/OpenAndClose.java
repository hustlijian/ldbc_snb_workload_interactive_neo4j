package com.ldbc.socialnet.neo4j.utils.testscripts;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.socialnet.neo4j.utils.Config;
import com.ldbc.socialnet.neo4j.utils.GraphStatistics;

public class OpenAndClose
{
    private final static Logger logger = Logger.getLogger( OpenAndClose.class );

    public static void main( String[] args ) throws IOException
    {
        OpenAndClose openCloser = new OpenAndClose( Config.DB_DIR );
        openCloser.openAndClose();
    }

    private final String dbDir;

    public OpenAndClose( String dbDir )
    {
        this.dbDir = dbDir;
    }

    public void openAndClose() throws IOException
    {
        logger.info( "Starting Neo4j" );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( dbDir );

        logger.info( "Calculating Graph Metrics:" );
        logger.info( "\tNode count = " + GraphStatistics.nodeCount( db, 10000000 ) );
        logger.info( "\tRelationship count = " + GraphStatistics.relationshipCount( db, 10000000 ) );

        db.shutdown();
    }
}
