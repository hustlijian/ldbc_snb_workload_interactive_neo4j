package com.ldbc.socialnet.workload.neo4j.load.tempindex;

public interface TempIndexFactory<K, V>
{
    public TempIndex<K, V> create();
}
