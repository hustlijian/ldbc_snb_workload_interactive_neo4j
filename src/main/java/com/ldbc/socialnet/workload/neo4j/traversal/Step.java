package com.ldbc.socialnet.workload.neo4j.traversal;

import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.neo4j.traversal.PropertyContainerFilterDescriptor.PropertyContainerPredicate;

public class Step
{
    private final Iterator<Relationship> NO_RELATIONSHIPS = Iterators.emptyIterator();
    private final PredicateGroup<PropertyContainer> startNodePredicates;
    private final PredicateGroup<PropertyContainer> relationshipPredicates;
    private final PredicateGroup<PropertyContainer> endNodePredicates;
    private final Function<Node, Iterator<Relationship>> expandFun;

    public Step( NodeFilterDescriptor startNodeFilters, RelationshipFilterDescriptor relationshipFilters,
            NodeFilterDescriptor endNodeFilters )
    {
        List<PropertyContainerPredicate> startNodePredicateArray = startNodeFilters.getFilterPredicates();
        if ( startNodePredicateArray.isEmpty() )
            startNodePredicates = null;
        else
            startNodePredicates = new PredicateGroup<PropertyContainer>(
                    startNodePredicateArray.toArray( new PropertyContainerPredicate[startNodePredicateArray.size()] ) );

        List<PropertyContainerPredicate> relationshipPredicateArray = relationshipFilters.getFilterPredicates();
        if ( relationshipPredicateArray.isEmpty() )
            relationshipPredicates = null;
        else
            relationshipPredicates = new PredicateGroup<PropertyContainer>(
                    relationshipPredicateArray.toArray( new PropertyContainerPredicate[relationshipPredicateArray.size()] ) );

        expandFun = relationshipFilters.expandFun();

        List<PropertyContainerPredicate> endNodePredicateArray = endNodeFilters.getFilterPredicates();
        if ( endNodePredicateArray.isEmpty() )
            endNodePredicates = null;
        else
            endNodePredicates = new PredicateGroup<PropertyContainer>(
                    endNodePredicateArray.toArray( new PropertyContainerPredicate[endNodePredicateArray.size()] ) );
    }

    Iterator<Relationship> take( Path path )
    {
        final Node startNode = path.endNode();

        if ( ( null != startNodePredicates ) && ( false == startNodePredicates.apply( startNode ) ) )
            return NO_RELATIONSHIPS;

        Iterator<Relationship> relationships = ( null == relationshipPredicates ) ? expandFun.apply( startNode )
                : Iterators.filter( expandFun.apply( startNode ), relationshipPredicates );

        if ( null == endNodePredicates ) return relationships;

        OtherNodePredicate otherNodePredicate = new OtherNodePredicate( startNode, endNodePredicates );
        return Iterators.filter( relationships, otherNodePredicate );
    }

    @Override
    public String toString()
    {
        return "Step [startNodePredicates=" + startNodePredicates + ", relationshipPredicates="
               + relationshipPredicates + ", endNodePredicates=" + endNodePredicates + ", expandFun=" + expandFun + "]";
    }

    static class OtherNodePredicate implements Predicate<Relationship>
    {
        private final Node startNode;
        private final PredicateGroup<PropertyContainer> endNodePredicates;

        public OtherNodePredicate( Node startNode, PredicateGroup<PropertyContainer> endNodePredicates )
        {
            this.startNode = startNode;
            this.endNodePredicates = endNodePredicates;
        }

        @Override
        public boolean apply( Relationship relationship )
        {
            Node otherNode = relationship.getOtherNode( startNode );
            return endNodePredicates.apply( otherNode );
        }
    };
}
