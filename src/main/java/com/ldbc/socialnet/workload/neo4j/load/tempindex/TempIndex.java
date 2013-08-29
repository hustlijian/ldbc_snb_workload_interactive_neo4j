package com.ldbc.socialnet.workload.neo4j.load.tempindex;

public interface TempIndex<K, V>
{
    public void put( K k, V v );

    public V get( K k );

    public void shutdown();
}
