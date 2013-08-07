package com.ldbc.socialnet.neo4j.tempindex;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Utils;

public class MapDbTempIndexFactory implements TempIndexFactory<Long, Long>
{
    @Override
    public TempIndex<Long, Long> create()
    {
        return new MapDbTempIndex();
    }

    public class MapDbTempIndex implements TempIndex<Long, Long>
    {
        private final HTreeMap<Long, Long> map;

        public MapDbTempIndex()
        {
            File dbFile = Utils.tempDbFile();
            DB db = DBMaker.newFileDB( dbFile ).closeOnJvmShutdown().make();
            this.map = db.createHashMap( "name", false, null, null );
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
    }
}
