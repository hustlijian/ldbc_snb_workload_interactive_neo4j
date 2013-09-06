package com.ldbc.socialnet.workload.neo4j.load.tempindex;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class MemoryMapDbTempIndexFactory implements TempIndexFactory<Long, Long>
{
    private Integer count = 0;

    @Override
    public TempIndex<Long, Long> create()
    {
        count++;
        String name = "mapdb" + count.toString();
        return new MemoryMapDbTempIndex( name );
    }

    public static class MemoryMapDbTempIndex implements TempIndex<Long, Long>
    {
        private HTreeMap<Long, Long> map;

        public MemoryMapDbTempIndex( String name )
        {
            DB db = DBMaker.newMemoryDB().writeAheadLogDisable().asyncFlushDelay( 100 ).closeOnJvmShutdown().make();
            this.map = db.createHashMap( name, false, null, null );
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
            map.close();
            map = null;
        }
    }
}
