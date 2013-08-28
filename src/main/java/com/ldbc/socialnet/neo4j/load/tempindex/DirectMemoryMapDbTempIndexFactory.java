package com.ldbc.socialnet.neo4j.load.tempindex;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class DirectMemoryMapDbTempIndexFactory implements TempIndexFactory<Long, Long>
{
    private Integer count = 0;

    @Override
    public TempIndex<Long, Long> create()
    {
        count++;
        String name = "mapdb" + count.toString();
        return new DirectMemoryMapDbTempIndex( name );
    }

    public static class DirectMemoryMapDbTempIndex implements TempIndex<Long, Long>
    {
        private HTreeMap<Long, Long> map;

        public DirectMemoryMapDbTempIndex( String name )
        {
            DB db = DBMaker.newDirectMemoryDB().writeAheadLogDisable().asyncFlushDelay( 100 ).closeOnJvmShutdown().make();
            this.map = db.getHashMap( name );
        }

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
            map.close();
            map = null;
        }
    }
}