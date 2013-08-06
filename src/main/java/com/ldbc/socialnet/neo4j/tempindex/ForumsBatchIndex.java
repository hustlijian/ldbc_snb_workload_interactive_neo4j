package com.ldbc.socialnet.neo4j.tempindex;

import gnu.trove.map.hash.TLongLongHashMap;

public class ForumsBatchIndex implements TempIndex<Long, Long>
{
    private final TLongLongHashMap map = new TLongLongHashMap();

    @Override
    public void put( Long k, Long v )
    {
        map.put( k, v );
    }

    @Override
    public Long get( Long k )
    {
        return map.get( k );
    }
}
