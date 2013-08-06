package com.ldbc.socialnet.neo4j;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

import com.ldbc.socialnet.neo4j.utils.Config;

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
        logger.info( "\tNode count = " + nodeCount( db ) );
        logger.info( "\tRelationship count = " + relationshipCount( db ) );

        db.shutdown();
    }

    private long nodeCount( GraphDatabaseService db )
    {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at( db );
        long nodeCount = -1;
        Transaction tx = db.beginTx();
        try
        {
            nodeCount = IteratorUtil.count( globalOperations.getAllNodes() );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getCause() );
        }
        finally
        {
            tx.finish();
        }
        return nodeCount;
    }

    private long relationshipCount( GraphDatabaseService db )
    {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at( db );
        long relationshipCount = -1;
        Transaction tx = db.beginTx();
        try
        {
            relationshipCount = IteratorUtil.count( globalOperations.getAllRelationships() );
            tx.success();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e.getCause() );
        }
        finally
        {
            tx.finish();
        }
        return relationshipCount;
    }
}
