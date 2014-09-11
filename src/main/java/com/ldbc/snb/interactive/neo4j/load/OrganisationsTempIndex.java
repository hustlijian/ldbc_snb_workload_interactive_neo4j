package com.ldbc.snb.interactive.neo4j.load;

import com.ldbc.snb.interactive.neo4j.load.tempindex.TempIndex;

public class OrganisationsTempIndex implements TempIndex<Long, Long>
{
    private final TempIndex<Long, Long> tempIndex;

    public OrganisationsTempIndex( TempIndex<Long, Long> tempIndex )
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
