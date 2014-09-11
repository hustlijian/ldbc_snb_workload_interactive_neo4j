package com.ldbc.snb.interactive.neo4j.load.tempindex;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

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
        count++;
        String name = "mapdb" + count.toString();
        File dbFile = new File( parentDir, name );
        return new PersistentMapDbTempIndex( dbFile, name );
    }

    public static class PersistentMapDbTempIndex implements TempIndex<Long, Long>
    {
        private HTreeMap<Long, Long> map;

        public PersistentMapDbTempIndex( File dbFile, String name )
        {
            DB db = DBMaker.newFileDB( dbFile ).writeAheadLogDisable().asyncFlushDelay( 100 ).closeOnJvmShutdown().make();
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
