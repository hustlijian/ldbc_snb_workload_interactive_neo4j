package com.ldbc.socialnet.workload.neo4j.traversal;

import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.PropertyContainer;

import com.google.common.base.Predicate;

public interface PropertyContainerFilterDescriptor<T extends PropertyContainerFilterDescriptor>
{
    public static abstract class PropertyContainerPredicate implements Predicate<PropertyContainer>
    {
    }

    List<PropertyContainerPredicate> getFilterPredicates();

    /**
     * AND semantics
     * 
     * @param propertyKey
     * @return
     */
    T hasPropertyKey( String propertyKey );

    Set<String> propertyKeys();

    /**
     * AND semantics
     * 
     * @param propertyKey
     * @param propertyValue
     * @return
     */
    T propertyEquals( String propertyKey, Object propertyValue );

    Set<PropertyValue> propertyValues();

    T conformsTo( PropertyContainerPredicate check );

    Set<PropertyContainerPredicate> genericChecks();
}
