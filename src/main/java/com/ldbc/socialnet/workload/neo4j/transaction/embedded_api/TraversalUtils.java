package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

// TODO test
public class TraversalUtils
{
    // TODO generalize to "aggregate" where input function defined what happens:
    // TODO count, group, distinct
    // TODO current impl -> Function<GROUP_TYPE, Map<GROUP_TYPE,INTEGER>>
    // overwrites aggregate
    // TODO then build helpers on top of "aggregate" -> count, group
    public static <THING> Map<THING, Integer> count( Iterator<THING> thingsToCount )
    {
        Map<THING, Integer> countedThings = new HashMap<THING, Integer>();
        while ( thingsToCount.hasNext() )
        {
            THING thing = thingsToCount.next();
            if ( countedThings.containsKey( thing ) )
            {
                int count = countedThings.get( thing );
                countedThings.put( thing, count + 1 );
            }
            else
            {
                countedThings.put( thing, 1 );
            }
        }
        return countedThings;
    }

    public static <THING> Iterator<THING> distinct( Iterator<THING> withDupicates )
    {
        Predicate<THING> distinctFun = new Predicate<THING>()
        {
            Set<THING> alreadySeen = new HashSet<THING>();

            @Override
            public boolean apply( THING input )
            {
                return alreadySeen.add( input );
            }
        };
        return Iterators.filter( withDupicates, distinctFun );
    }
}
