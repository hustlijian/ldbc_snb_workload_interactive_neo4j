package com.ldbc.socialnet.neo4j.domain;

import com.ldbc.socialnet.neo4j.tempindex.TempIndex;

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
}
