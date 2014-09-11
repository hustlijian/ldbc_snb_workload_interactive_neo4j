package com.ldbc.snb.interactive.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.snb.interactive.neo4j.Domain;
import com.ldbc.snb.interactive.neo4j.interactive.LdbcTraversers;
import com.ldbc.snb.interactive.neo4j.interactive.Neo4jQuery11;
import org.neo4j.graphdb.*;
import org.neo4j.traversal.steps.execution.StepsUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery11EmbeddedApi extends Neo4jQuery11<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery11EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query11 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery11Result> execute(GraphDatabaseService db, LdbcQuery11 operation) {
        Iterator<Node> personIterator = db.findNodesByLabelAndProperty(Domain.Nodes.Person, Domain.Person.ID, operation.personId()).iterator();
        if (false == personIterator.hasNext()) return Iterators.emptyIterator();
        final Node person = personIterator.next();

        List<Node> friendsList = ImmutableList.copyOf(
                StepsUtils.excluding(
                        StepsUtils.distinct(
                                traversers.friendsAndFriendsOfFriends().traverse(person).nodes()
                        ),
                        person
                )
        );
        Node[] friends = friendsList.toArray(new Node[friendsList.size()]);

        List<LdbcQuery11Result> results = Lists.newArrayList(
                Iterators.transform(
                        traversers.companiesPersonWorkedAtInGivenCountryBeforeGivenDate(operation.countryName(), operation.workFromYear()).traverse(friends).iterator(),
                        new Function<Path, LdbcQuery11Result>() {
                            @Override
                            public LdbcQuery11Result apply(Path path) {
                                List<PropertyContainer> pathElements = Lists.newArrayList(path);
                                Node person = (Node) pathElements.get(0);
                                Relationship workedAt = (Relationship) pathElements.get(1);
                                Node organization = (Node) pathElements.get(2);
                                long personId = (long) person.getProperty(Domain.Person.ID);
                                String personFirstName = (String) person.getProperty(Domain.Person.FIRST_NAME);
                                String personLastName = (String) person.getProperty(Domain.Person.LAST_NAME);
                                String organizationName = (String) organization.getProperty(Domain.Organisation.NAME);
                                int organizationWorkFromYear = (int) workedAt.getProperty(Domain.WorksAt.WORK_FROM);
                                return new LdbcQuery11Result(personId, personFirstName, personLastName, organizationName, organizationWorkFromYear);
                            }
                        }
                )
        );

        Collections.sort(results, new AscendingWorkFromYearAscendingPersonIdentifierDescendingOrganizationName());
        return Iterators.limit(results.iterator(), operation.limit());
    }

    public static class AscendingWorkFromYearAscendingPersonIdentifierDescendingOrganizationName implements Comparator<LdbcQuery11Result> {
        @Override
        public int compare(LdbcQuery11Result result1, LdbcQuery11Result result2) {
            if (result1.organizationWorkFromYear() < result2.organizationWorkFromYear()) return -1;
            else if (result1.organizationWorkFromYear() > result2.organizationWorkFromYear()) return 1;
            else {
                if (result1.personId() < result2.personId()) return -1;
                else if (result1.personId() > result2.personId()) return 1;
                else {
                    return result1.organizationName().compareTo(result2.organizationName());
                }
            }
        }
    }
}
