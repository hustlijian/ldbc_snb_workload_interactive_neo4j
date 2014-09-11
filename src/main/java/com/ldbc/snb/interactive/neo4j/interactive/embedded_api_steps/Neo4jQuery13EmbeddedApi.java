package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery13;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;

import java.util.Iterator;

public class Neo4jQuery13EmbeddedApi extends Neo4jQuery13<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery13EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query13 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery13Result> execute(GraphDatabaseService db, LdbcQuery13 operation) {
        Iterator<Node> person1Iterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.person1Id()).iterator();
        if (false == person1Iterator.hasNext()) return Iterators.emptyIterator();
        final Node person1 = person1Iterator.next();

        Iterator<Node> person2Iterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.person2Id()).iterator();
        if (false == person2Iterator.hasNext()) return Iterators.emptyIterator();
        final Node person2 = person2Iterator.next();

        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(PathExpanders.forTypeAndDirection(Domain.Rels.KNOWS, Direction.BOTH), Integer.MAX_VALUE);
        Path shortestPath = finder.findSinglePath(person1, person2);

        int shortestPathLength = (null == shortestPath) ? -1 : shortestPath.length();
        return Lists.newArrayList(new LdbcQuery13Result(shortestPathLength)).iterator();
    }
}
