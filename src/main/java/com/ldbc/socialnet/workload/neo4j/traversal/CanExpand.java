package com.ldbc.socialnet.workload.neo4j.traversal;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import com.google.common.base.Function;

public interface CanExpand
{
    Function<Node, Iterator<Relationship>> expandFun();
}
