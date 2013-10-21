package com.ldbc.socialnet.workload.neo4j.traversal;

import java.util.Iterator;

import org.neo4j.cypher.internal.symbols.RelationshipType;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;

import com.google.common.collect.ImmutableList;

public class StepsExpander implements PathExpander<RelationshipType>
{
    private final Step[] steps;

    public StepsExpander( Step... steps )
    {
        this.steps = steps;
    }

    @Override
    public Iterable<Relationship> expand( Path path, BranchState<RelationshipType> state )
    {
        Step step = steps[path.length()];
        /*
         * Return List because Iterable should be capable of returning multiple Iterator instances
         * Wrapping Iterator in an Iterable that can only consume it once would break the Iterable contract
         */
        return ImmutableList.copyOf( step.take( path ) );
    }

    @Override
    public PathExpander<RelationshipType> reverse()
    {
        throw new UnsupportedOperationException( "reverse not implemented by " + getClass().getSimpleName() );
    }
}
