package com.ldbc.socialnet.workload.neo4j.interactive.embedded_api_steps;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.Domain;
import com.ldbc.socialnet.workload.neo4j.interactive.LdbcTraversers;
import com.ldbc.socialnet.workload.neo4j.interactive.Neo4jQuery1;
import org.neo4j.graphdb.*;

import java.util.*;

public class Neo4jQuery1EmbeddedApi extends Neo4jQuery1<GraphDatabaseService> {
    private static final PersonLastNameAndIdComparator personLastNameAndIdComparator = new PersonLastNameAndIdComparator();
    private final Query1ResultProjectionFunction query1ResultProjectionFunction;

    public Neo4jQuery1EmbeddedApi(LdbcTraversers traversers) {
        this.query1ResultProjectionFunction = new Query1ResultProjectionFunction(traversers);
    }

    @Override
    public String description() {
        return "LDBC Query1 Java API Implementation";
    }

    @Override
    public Iterator<LdbcQuery1Result> execute(GraphDatabaseService db, LdbcQuery1 params) {
        /*
        Given a start Person, find up to 20 Persons with a given first name that the start Person is connected to (excluding start Person) by at most 3 steps via Knows relationships.
        Return Persons, including summaries of the Persons’ workplaces and places of study.
        Sort results by their distance from the start Person, for Persons within the same distance sort by their last name, and for Persons with same last name by their identifier
         */
        List<Node> persons = Lists.newArrayList(
                db.findNodesByLabelAndProperty(
                        Domain.Nodes.Person,
                        Domain.Person.ID,
                        params.personId()
                )
        );
        if (persons.isEmpty()) return Iterators.emptyIterator();
        final Node startPerson = persons.get(0);

        // friends
        Set<Node> personsAtDistance1 = Sets.newHashSet();
        List<NodeAndDistance> friends = Lists.newArrayList();
        for (Relationship rel : startPerson.getRelationships(Domain.Rels.KNOWS)) {
            Node person = rel.getOtherNode(startPerson);
            if (person.hasLabel(Domain.Nodes.Person)
                    && params.firstName().equals(person.getProperty(Domain.Person.FIRST_NAME))
                    && false == personsAtDistance1.contains(person))
                friends.add(new NodeAndDistance(person, 1));
            personsAtDistance1.add(person);
        }
        Collections.sort(friends, personLastNameAndIdComparator);

        // friends of friends
        if (params.limit() > friends.size()) {
            Set<Node> personsAtDistance2 = Sets.newHashSet();
            List<NodeAndDistance> friendsOfFriends = Lists.newArrayList();
            for (Node personAtDistance1 : personsAtDistance1) {
                for (Relationship rel : personAtDistance1.getRelationships(Domain.Rels.KNOWS)) {
                    Node friendOfFriend = rel.getOtherNode(personAtDistance1);
                    if (friendOfFriend.hasLabel(Domain.Nodes.Person)
                            && params.firstName().equals(friendOfFriend.getProperty(Domain.Person.FIRST_NAME))
                            && false == personsAtDistance1.contains(friendOfFriend)
                            && false == personsAtDistance2.contains(friendOfFriend))
                        friendsOfFriends.add(new NodeAndDistance(friendOfFriend, 2));
                    personsAtDistance2.add(friendOfFriend);
                }
            }
            Collections.sort(friendsOfFriends, personLastNameAndIdComparator);
            friends.addAll(friendsOfFriends);

            // friends of friends of friends
            if (params.limit() > friends.size()) {
                Set<Node> personsAtDistance3 = Sets.newHashSet();
                List<NodeAndDistance> friendsOfFriendsOfFriends = Lists.newArrayList();
                for (Node personAtDistance2 : personsAtDistance2) {
                    for (Relationship rel : personAtDistance2.getRelationships(Domain.Rels.KNOWS)) {
                        Node friendOfFriendOfFriend = rel.getOtherNode(personAtDistance2);
                        if (friendOfFriendOfFriend.hasLabel(Domain.Nodes.Person)
                                && params.firstName().equals(friendOfFriendOfFriend.getProperty(Domain.Person.FIRST_NAME))
                                && false == personsAtDistance1.contains(friendOfFriendOfFriend)
                                && false == personsAtDistance2.contains(friendOfFriendOfFriend)
                                && false == personsAtDistance3.contains(friendOfFriendOfFriend))
                            friendsOfFriendsOfFriends.add(new NodeAndDistance(friendOfFriendOfFriend, 3));
                        personsAtDistance3.add(friendOfFriendOfFriend);
                    }
                }
                Collections.sort(friendsOfFriendsOfFriends, personLastNameAndIdComparator);
                friends.addAll(friendsOfFriendsOfFriends);
            }
        }

        Iterator<NodeAndDistance> allFriends = Iterators.limit(friends.iterator(), params.limit());
        return Iterators.transform(allFriends, query1ResultProjectionFunction);
    }

    private static class NodeAndDistance {
        private final Node node;
        private final int distance;

        private NodeAndDistance(Node node, int distance) {
            this.node = node;
            this.distance = distance;
        }

        public Node node() {
            return node;
        }

        public int distance() {
            return distance;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NodeAndDistance that = (NodeAndDistance) o;

            if (node != null ? !node.equals(that.node) : that.node != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return node != null ? node.hashCode() : 0;
        }
    }

    private static class PersonLastNameAndIdComparator implements Comparator<NodeAndDistance> {
        @Override
        public int compare(NodeAndDistance nodeAndDistance1, NodeAndDistance nodeAndDistance2) {
            // first sort by last name
            String endNode1LastName = (String) nodeAndDistance1.node().getProperty(Domain.Person.LAST_NAME);
            String endNode2LastName = (String) nodeAndDistance2.node().getProperty(Domain.Person.LAST_NAME);
            int lastNameCompare = (endNode1LastName.compareTo(endNode2LastName));
            if (0 != lastNameCompare) return lastNameCompare;
            // then sort by id
            long endNode1Id = (long) nodeAndDistance1.node().getProperty(Domain.Person.ID);
            long endNode2Id = (long) nodeAndDistance2.node().getProperty(Domain.Person.ID);
            if (endNode1Id > endNode2Id) return 1;
            if (endNode1Id < endNode2Id) return -1;
            return 0;
        }
    }

    private static class Query1ResultProjectionFunction implements Function<NodeAndDistance, LdbcQuery1Result> {
        private final LdbcTraversers traversers;

        private Query1ResultProjectionFunction(LdbcTraversers traversers) {
            this.traversers = traversers;
        }

        @Override
        public LdbcQuery1Result apply(NodeAndDistance personAndPathLength) {
            Node person = personAndPathLength.node();
            Integer pathLength = personAndPathLength.distance();
            long id = (long) person.getProperty(Domain.Person.ID);
            String lastName = (String) person.getProperty(Domain.Person.LAST_NAME);
            long birthday = (long) person.getProperty(Domain.Person.BIRTHDAY);
            long creationDate = (long) person.getProperty(Domain.Person.CREATION_DATE);
            String gender = (String) person.getProperty(Domain.Person.GENDER);
            Iterable<String> languages = Lists.newArrayList((String[]) person.getProperty(Domain.Person.LANGUAGES));
            String browser = (String) person.getProperty(Domain.Person.BROWSER_USED);
            String ip = (String) person.getProperty(Domain.Person.LOCATION_IP);
            Iterable<String> emails = Lists.newArrayList((String[]) person.getProperty(Domain.Person.EMAIL_ADDRESSES));
            String personCity = (String) person.getSingleRelationship(Domain.Rels.IS_LOCATED_IN, Direction.OUTGOING).getEndNode().getProperty(
                    Domain.Place.NAME);

            // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
            List<List<Object>> unis = Lists.newArrayList(Iterables.transform(
                    traversers.personUniversities().traverse(person), new Function<Path, List<Object>>() {
                        @Override
                        public List<Object> apply(Path input) {
                            List<Node> nodes = Lists.newArrayList(input.nodes());
                            List<Relationship> relationships = Lists.newArrayList(input.relationships());
                            return Lists.newArrayList(
                                    nodes.get(1).getProperty(Domain.Organisation.NAME),
                                    relationships.get(0).getProperty(Domain.StudiesAt.CLASS_YEAR),
                                    nodes.get(2).getProperty(Domain.Place.NAME)
                            );
                        }
                    }));

            // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
            List<List<Object>> companies = Lists.newArrayList(Iterables.transform(
                    traversers.personCompanies().traverse(person), new Function<Path, List<Object>>() {
                        @Override
                        public List<Object> apply(Path input) {
                            List<Node> nodes = Lists.newArrayList(input.nodes());
                            List<Relationship> relationships = Lists.newArrayList(input.relationships());
                            return Lists.newArrayList(
                                    nodes.get(1).getProperty(Domain.Organisation.NAME),
                                    relationships.get(0).getProperty(Domain.WorksAt.WORK_FROM),
                                    nodes.get(2).getProperty(Domain.Place.NAME)
                            );
                        }
                    }));

            return new LdbcQuery1Result(
                    id,
                    lastName,
                    pathLength,
                    birthday,
                    creationDate,
                    gender,
                    browser,
                    ip,
                    emails,
                    languages,
                    personCity,
                    unis,
                    companies);
        }
    }
}
