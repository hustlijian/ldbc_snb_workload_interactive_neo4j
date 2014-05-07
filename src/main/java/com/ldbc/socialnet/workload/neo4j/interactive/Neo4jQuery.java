package com.ldbc.socialnet.workload.neo4j.interactive;

import java.util.Iterator;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.ldbc.driver.Operation;

public interface Neo4jQuery<INPUT extends Operation<?>, OUTPUT>
{
    String description();

    Iterator<OUTPUT> execute( GraphDatabaseService db, ExecutionEngine engine, INPUT params );
}
