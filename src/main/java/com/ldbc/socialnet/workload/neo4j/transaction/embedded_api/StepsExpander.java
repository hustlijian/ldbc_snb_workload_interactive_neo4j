package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api;

import java.util.ArrayList;

import org.neo4j.cypher.internal.symbols.RelationshipType;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;

import com.ldbc.driver.util.Function1;

public class StepsExpander implements PathExpander<RelationshipType>
{
    private static final Iterable<Relationship> EMPTY_ITERABLE = new ArrayList<Relationship>();
    private final Function1<Path, Iterable<Relationship>>[] steps;

    public StepsExpander( Function1<Path, Iterable<Relationship>>... steps )
    {
        this.steps = steps;
    }

    @Override
    public Iterable<Relationship> expand( Path path, BranchState<RelationshipType> state )
    {
        Iterable<Relationship> expansion = steps[path.length()].apply( path );
        return ( null == expansion ) ? EMPTY_ITERABLE : expansion;
    }

    @Override
    public PathExpander<RelationshipType> reverse()
    {
        return null;
    }

}
