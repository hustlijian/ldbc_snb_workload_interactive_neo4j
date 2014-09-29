package com.ldbc.snb.interactive.neo4j.load.tempindex;

import gnu.trove.map.hash.TLongLongHashMap;

public class TroveTempIndexFactory implements TempIndexFactory<Long, Long>
{
    @Override
    public TempIndex<Long, Long> create()
    {
        return new TroveTempIndex();
    }

    public static class TroveTempIndex implements TempIndex<Long, Long>
    {
        private TLongLongHashMap map = new TLongLongHashMap();

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

        @Override
        public void shutdown()
        {
            map = null;
        }
    }
}
