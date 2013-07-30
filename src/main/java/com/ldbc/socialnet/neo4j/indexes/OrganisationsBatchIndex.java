package com.ldbc.socialnet.neo4j.indexes;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

public class OrganisationsBatchIndex
{
    private final BatchInserterIndex index;

    public OrganisationsBatchIndex( BatchInserterIndexProvider batchIndexProvider )
    {
        index = batchIndexProvider.nodeIndex( "organisations", MapUtil.stringMap( "type", "exact" ) );
        // .setCacheCapacity( "name", 100000 );
    }

    public BatchInserterIndex getIndex()
    {
        return index;
    }
}
