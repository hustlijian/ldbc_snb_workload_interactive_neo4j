package com.ldbc.socialnet.neo4j;

import java.util.Comparator;

public abstract class CsvLineInserter
{
    public Object[] transform( Object[] columnValues )
    {
        return columnValues;
    }

    public Comparator<Object[]> sortComparator()
    {
        return null;
    }

    public abstract void insert( Object[] columnValues );
}
