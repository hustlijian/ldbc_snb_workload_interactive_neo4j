package com.ldbc.socialnet.workload.neo4j.interactive;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.util.Set;

public interface LdbcTraversers {
    TraversalDescription personUniversities();

    TraversalDescription personCompanies();

    TraversalDescription friendPostTags(final long minDate, final long maxDate);

    TraversalDescription friendsPostsBeforeDate(final long maxPostCreationDate);

    TraversalDescription friendsUpToThreeHopsWithGivenFirstName(String firstName);

    TraversalDescription friendsAndFriendsOfFriends();

    TraversalDescription personsPostsWithGivenTag(final String tagName);

    TraversalDescription tagsOnPosts(final String tagName);

    TraversalDescription postsInCountry(final String countryX, final long minDate, final long maxDate);

    TraversalDescription personsMembershipForums(final long minDate);

    TraversalDescription personsComments();

    TraversalDescription commentsOnPostsInForum(final Set<Node> comments);

    TraversalDescription postsInForumByFriends(final Set<Node> knownPersons);

    TraversalDescription postsInPersonsCountryInDateRangeNotCreatedByOtherPerson(final Node otherPerson);

    TraversalDescription postsTags();
}
