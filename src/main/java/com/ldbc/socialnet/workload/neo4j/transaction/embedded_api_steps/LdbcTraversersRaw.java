package com.ldbc.socialnet.workload.neo4j.transaction.embedded_api_steps;

import com.ldbc.socialnet.workload.neo4j.transaction.LdbcTraversers;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.util.Set;

public class LdbcTraversersRaw implements LdbcTraversers {
    private final GraphDatabaseService db;

    public LdbcTraversersRaw(GraphDatabaseService db) {
        this.db = db;
    }

    // (uniCity:CITY)<-[:IS_LOCATED_IN]-(uni:UNIVERSITY)<-[studyAt:STUDY_AT]-(person)
    // uni.name, uniCity.name(studyAt.classYear)
    public TraversalDescription personUniversities() {
        throw new UnsupportedOperationException();
    }

    // (companyCountry:PLACE:COUNTRY)<-[:IS_LOCATED_IN]-(company:COMPANY)<-[worksAt:WORKS_AT]-(person)
    // company.name, companyCountry.name(worksAt.workFrom)
    public TraversalDescription personCompanies() {
        throw new UnsupportedOperationException();
    }

    /*
        MATCH (person:PERSON)-[:KNOWS]-(friend:PERSON)
        MATCH (friend)<-[:HAS_CREATOR]-(post:POST)
        WHERE post.creationDate>={min_date} AND post.creationDate<={max_date}
        MATCH (post)-[HAS_TAG]->(tag:TAG)
     */
    public TraversalDescription friendPostTags(final long minDate, final long maxDate) {
        throw new UnsupportedOperationException();
    }

    /*
    MATCH (person:PERSON)-[:KNOWS*1..2]-(f:PERSON)
     */
    public TraversalDescription friendsAndFriendsOfFriends() {
        throw new UnsupportedOperationException();
    }

    /*
        MATCH (friend)<-[:HAS_CREATOR]-(postX:POST)-[:IS_LOCATED_IN]->(countryX:COUNTRY)
        WHERE countryX.name={country_x} AND postX.creationDate>={min_date} AND postX.creationDate<={max_date}
     */
    public TraversalDescription postsInCountry(final String countryX, final long minDate, final long maxDate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TraversalDescription personsMembershipForums(long minDate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TraversalDescription personsComments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TraversalDescription commentsOnPostsInForum(Set<Node> forums) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TraversalDescription postsInForumByFriends(Set<Node> knownPersons) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TraversalDescription friendsPostsBeforeDate(long maxPostCreationDate) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TraversalDescription tagsOnPosts(String tagName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TraversalDescription personsPostsWithGivenTag(String tagName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TraversalDescription postsInPersonsCountryInDateRangeNotCreatedByOtherPerson(Node otherPerson) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TraversalDescription postsTags() {
        // TODO Auto-generated method stub
        return null;
    }
}
