package com.ldbc.socialnet.neo4j.load.tempindex;

public interface TempIndexFactory<K, V>
{
    public TempIndex<K, V> create();
}
