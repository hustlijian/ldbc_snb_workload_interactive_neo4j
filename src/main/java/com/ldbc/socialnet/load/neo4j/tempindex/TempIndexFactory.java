package com.ldbc.socialnet.load.neo4j.tempindex;

public interface TempIndexFactory<K, V>
{
    public TempIndex<K, V> create();
}
