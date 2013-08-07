package com.ldbc.socialnet.neo4j.tempindex;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Utils;

public class PersistentMapDbTempIndexFactory implements TempIndexFactory<Long, Long>
{
    private final File parentDir;
    private Integer count = 0;

    public PersistentMapDbTempIndexFactory( File parentDir )
    {
        this.parentDir = parentDir;
    }

    @Override
    public TempIndex<Long, Long> create()
    {
        File dbFile = new File( parentDir, "mapdb" + count.toString() );
        return new MapDbTempIndex( dbFile );
    }

    public static class MapDbTempIndex implements TempIndex<Long, Long>
    {
        private final HTreeMap<Long, Long> map;

        public MapDbTempIndex( File dbFile )
        {
            DB db = DBMaker.newFileDB( dbFile ).writeAheadLogDisable().closeOnJvmShutdown().make();
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
