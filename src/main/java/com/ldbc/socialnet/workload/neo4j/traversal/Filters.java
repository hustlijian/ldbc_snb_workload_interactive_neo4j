package com.ldbc.socialnet.workload.neo4j.traversal;

public class Filters
{
    public static NodeFilterDescriptor node()
    {
        return new NodeFilterDescriptor();
    }

    public static RelationshipFilterDescriptor relationship()
    {
        return new RelationshipFilterDescriptor();
    }
}
