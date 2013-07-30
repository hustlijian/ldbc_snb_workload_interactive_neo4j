package com.ldbc.socialnet.neo4j;

public abstract class CsvLineInserter
{
    public Object[] transform( Object[] columnValues )
    {
        return columnValues;
    }

    public abstract void insert( Object[] columnValues );
}
