package com.ldbc.socialnet.workload.neo4j.load.tempindex;

import java.util.HashMap;
import java.util.Map;

public class HashMapTempIndexFactory implements TempIndexFactory<Long, Long>
{
    @Override
    public TempIndex<Long, Long> create()
    {
        return new HashMapTempIndex();
    }

    public static class HashMapTempIndex implements TempIndex<Long, Long>
    {
        private Map<Long, Long> map = new HashMap<Long, Long>();

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
            map.clear();
            map = null;
        }
    }
}
