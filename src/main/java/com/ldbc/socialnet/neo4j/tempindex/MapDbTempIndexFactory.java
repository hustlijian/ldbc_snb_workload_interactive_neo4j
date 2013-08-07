package com.ldbc.socialnet.neo4j.tempindex;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Utils;

/*
then if nothing happens in 30 minutes I'll move back to Trove (i moved away from Trove only due to memory limitations, but maybe you can help me configure the importer to use less memory) */

public class MapDbTempIndexFactory implements TempIndexFactory<Long, Long>
{
    @Override
    public TempIndex<Long, Long> create()
    {
        return new MapDbTempIndex();
    }

    public static class MapDbTempIndex implements TempIndex<Long, Long>
    {
        private final HTreeMap<Long, Long> map;

        public MapDbTempIndex()
        {
            File dbFile = Utils.tempDbFile();
            DB db = DBMaker.newFileDB( dbFile ).writeAheadLogDisable().closeOnJvmShutdown().make();
            this.map = db.createHashMap( "name", false, null, null );

            // DB db1 =
            // DBMaker.newDirectMemoryDB().transactionDisable().asyncFlushDelay(
            // 100 ).make();
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
