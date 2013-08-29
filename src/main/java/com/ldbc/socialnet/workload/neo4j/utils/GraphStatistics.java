package com.ldbc.socialnet.workload.neo4j.utils;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

public class GraphStatistics
{
    public static long nodeCount( GraphDatabaseService db, long transactionSize )
    {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at( db );
        long nodeCount = 0;
        Transaction tx = db.beginTx();
        try
        {
            for ( Node node : globalOperations.getAllNodes() )
            {
                nodeCount++;
                if ( nodeCount % transactionSize == 0 )
                {
                    tx.success();
                    tx.finish();
                    tx = db.beginTx();
                }
            }
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

    public static long relationshipCount( GraphDatabaseService db, long transactionSize )
    {
        GlobalGraphOperations globalOperations = GlobalGraphOperations.at( db );
        long relationshipCount = 0;
        Transaction tx = db.beginTx();
        try
        {
            for ( Relationship relationship : globalOperations.getAllRelationships() )
            {
                relationshipCount++;
                if ( relationshipCount % transactionSize == 0 )
                {
                    tx.success();
                    tx.finish();
                    tx = db.beginTx();
                }
            }
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
