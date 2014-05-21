package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery1;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class Neo4jQuery1EmbeddedApi extends Neo4jQuery1<GraphDatabaseService> {
    private final LdbcTraversers traversers;

    public Neo4jQuery1EmbeddedApi(LdbcTraversers traversers) {
        this.traversers = traversers;
    }

    @Override
    public String description() {
        return "LDBC Query1 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery1Result> execute(GraphDatabaseService db, LdbcQuery1 params) {
        /*
        Given a person’s first name, return up to 10 people with the same first name sorted by last name. 
        Persons are returned (e.g. as for a search page with top 10 shown), 
        and the information is complemented with summaries of the persons' workplaces, places of study, etc.
         */
        List<Node> firstNamePersons = Lists.newArrayList(db.findNodesByLabelAndProperty(Domain.Nodes.Person,
                Domain.Person.FIRST_NAME, params.firstName()).iterator());
        Collections.sort(firstNamePersons, new LastNameComparator());
        return Iterators.transform(firstNamePersons.iterator(), new Query1ResultProjectionFunction());
    }

    public static class LastNameComparator implements Comparator<Node> {
        @Override
        public int compare(Node person1, Node person2) {
            String person1LastName = (String) person1.getProperty(Domain.Person.LAST_NAME);
            String person2LastName = (String) person2.getProperty(Domain.Person.LAST_NAME);
            return person1LastName.compareTo(person2LastName);
        }
    }

    private class Query1ResultProjectionFunction implements Function<Node, LdbcQuery1Result> {
        @Override
        public LdbcQuery1Result apply(Node person) {
//            String firstName = (String) person.getProperty(Domain.Person.FIRST_NAME);
//            String lastName = (String) person.getProperty(Domain.Person.LAST_NAME);
//            long birthday = (long) person.getProperty(Domain.Person.BIRTHDAY);
//            long creationDate = (long) person.getProperty(Domain.Person.CREATION_DATE);
//            String gender = (String) person.getProperty(Domain.Person.GENDER);
//            String[] languages = (String[]) person.getProperty(Domain.Person.LANGUAGES);
//            String browser = (String) person.getProperty(Domain.Person.BROWSER_USED);
//            String ip = (String) person.getProperty(Domain.Person.LOCATION_IP);
//            String[] emails = (String[]) person.getProperty(Domain.Person.EMAIL_ADDRESSES);
//            String personCity = (String) person.getSingleRelationship(Domain.Rels.IS_LOCATED_IN, Direction.OUTGOING).getEndNode().getProperty(
//                    Domain.Place.NAME);
//            // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
//            Collection<String> unis = Lists.newArrayList(Iterables.transform(
//                    traversers.personUniversities().traverse(person), new Function<Path, String>() {
//                @Override
//                public String apply(Path input) {
//                    List<Node> nodes = Lists.newArrayList(input.nodes());
//                    List<Relationship> relationships = Lists.newArrayList(input.relationships());
//                    return String.format("%s, %s(%s)", nodes.get(1).getProperty(Domain.Organisation.NAME),
//                            nodes.get(2).getProperty(Domain.Place.NAME),
//                            relationships.get(0).getProperty(Domain.StudiesAt.CLASS_YEAR));
//                }
//            }));
//
//            // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
//            Collection<String> companies = Lists.newArrayList(Iterables.transform(
//                    traversers.personCompanies().traverse(person), new Function<Path, String>() {
//                @Override
//                public String apply(Path input) {
//                    List<Node> nodes = Lists.newArrayList(input.nodes());
//                    List<Relationship> relationships = Lists.newArrayList(input.relationships());
//                    return String.format("%s, %s(%s)", nodes.get(1).getProperty(Domain.Organisation.NAME),
//                            nodes.get(2).getProperty(Domain.Place.NAME),
//                            relationships.get(0).getProperty(Domain.WorksAt.WORK_FROM));
//                }
//            }));
//            return new LdbcQuery1Result(firstName, lastName, birthday, creationDate, gender, languages, browser, ip,
//                    emails, personCity, unis, companies);
            return null;
        }
    }
}
