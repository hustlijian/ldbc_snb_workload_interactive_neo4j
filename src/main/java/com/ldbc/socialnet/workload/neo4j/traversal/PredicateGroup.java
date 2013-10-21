package com.ldbc.socialnet.workload.neo4j.traversal;

import java.util.Arrays;

import com.google.common.base.Predicate;

public class PredicateGroup<T> implements Predicate<T>
{
    private final Predicate<T>[] checks;

    PredicateGroup( Predicate<T>[] checks )
    {
        this.checks = checks;
    }

    @Override
    public boolean apply( T input )
    {
        for ( int i = 0; i < checks.length; i++ )
        {
            if ( false == checks[i].apply( input ) ) return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "PredicateGroup [checks=" + Arrays.toString( checks ) + "]";
    }
}
