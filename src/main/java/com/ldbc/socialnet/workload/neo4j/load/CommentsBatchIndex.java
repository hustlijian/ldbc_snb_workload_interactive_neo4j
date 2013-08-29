package com.ldbc.socialnet.workload.neo4j.load;

import com.ldbc.socialnet.workload.neo4j.load.tempindex.TempIndex;

public class CommentsBatchIndex implements TempIndex<Long, Long>
{
    private final TempIndex<Long, Long> tempIndex;

    public CommentsBatchIndex( TempIndex<Long, Long> tempIndex )
    {
        this.tempIndex = tempIndex;
    }

    @Override
    public void put( Long k, Long v )
    {
        tempIndex.put( k, v );
    }

    @Override
    public Long get( Long k )
    {
        return tempIndex.get( k );
    }

    @Override
    public void shutdown()
    {
        tempIndex.shutdown();
    }
}
