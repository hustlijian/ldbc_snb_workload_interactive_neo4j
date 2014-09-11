package com.ldbc.snb.interactive.neo4j.interactive;

import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;

import java.util.Iterator;

public interface Neo4jQuery<INPUT extends Operation<?>, OUTPUT, CONNECTION> {
    String description();

    Iterator<OUTPUT> execute(CONNECTION connection, INPUT params) throws DbException;
}
