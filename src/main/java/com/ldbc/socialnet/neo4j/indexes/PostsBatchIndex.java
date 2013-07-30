package com.ldbc.socialnet.neo4j.indexes;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

public class PostsBatchIndex
{
    private final BatchInserterIndex index;

    public PostsBatchIndex( BatchInserterIndexProvider batchIndexProvider )
    {
        index = batchIndexProvider.nodeIndex( "posts", MapUtil.stringMap( "type", "exact" ) );
        // .setCacheCapacity( "name", 100000 );
    }

    public BatchInserterIndex getIndex()
    {
        return index;
    }
}
