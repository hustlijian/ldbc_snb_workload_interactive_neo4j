package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public abstract class QueryCorrectnessTest<CONNECTION> implements QueryCorrectnessTestImplProvider<CONNECTION> {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    public abstract CONNECTION openConnection(String path) throws Exception;

    public abstract void closeConnection(CONNECTION connection) throws Exception;

    public abstract Iterator<LdbcQuery1Result> neo4jQuery1Impl(CONNECTION connection, LdbcQuery1 operation) throws Exception;

    public abstract Iterator<LdbcQuery2Result> neo4jQuery2Impl(CONNECTION connection, LdbcQuery2 operation) throws Exception;

    public abstract Iterator<LdbcQuery3Result> neo4jQuery3Impl(CONNECTION connection, LdbcQuery3 operation) throws Exception;

    public abstract Iterator<LdbcQuery4Result> neo4jQuery4Impl(CONNECTION connection, LdbcQuery4 operation) throws Exception;

    public abstract Iterator<LdbcQuery5Result> neo4jQuery5Impl(CONNECTION connection, LdbcQuery5 operation) throws Exception;

    public abstract Iterator<LdbcQuery6Result> neo4jQuery6Impl(CONNECTION connection, LdbcQuery6 operation) throws Exception;

    public abstract Iterator<LdbcQuery7Result> neo4jQuery7Impl(CONNECTION connection, LdbcQuery7 operation) throws Exception;

    public abstract Iterator<LdbcQuery8Result> neo4jQuery8Impl(CONNECTION connection, LdbcQuery8 operation) throws Exception;

    public abstract Iterator<LdbcQuery9Result> neo4jQuery9Impl(CONNECTION connection, LdbcQuery9 operation) throws Exception;

    public abstract Iterator<LdbcQuery10Result> neo4jQuery10Impl(CONNECTION connection, LdbcQuery10 operation) throws Exception;

    public abstract Iterator<LdbcQuery11Result> neo4jQuery11Impl(CONNECTION connection, LdbcQuery11 operation) throws Exception;

    public abstract Iterator<LdbcQuery12Result> neo4jQuery12Impl(CONNECTION connection, LdbcQuery12 operation) throws Exception;

    public abstract Iterator<LdbcQuery13Result> neo4jQuery13Impl(CONNECTION connection, LdbcQuery13 operation) throws Exception;

    public abstract Iterator<LdbcQuery14Result> neo4jQuery14Impl(CONNECTION connection, LdbcQuery14 operation) throws Exception;

    @Test
    public void query1ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 0;
            String personUri = null;
            String friendName = "name0";
            int limit = 6;
            LdbcQuery1 operation = new LdbcQuery1(personId, personUri, friendName, limit);

            Iterator<LdbcQuery1Result> result;
            LdbcQuery1Result actualRow;
            LdbcQuery1Result expectedRow;

            result = neo4jQuery1Impl(connection, operation);

            actualRow = result.next();

            // TODO remove
            System.out.println(actualRow.toString());

            expectedRow = new LdbcQuery1Result(
                    2L,
                    "last0",
                    1,
                    2L,
                    2L,
                    "gender2",
                    "browser2",
                    "ip2",
                    Sets.<String>newHashSet(),
                    Sets.newHashSet("friend2language0", "friend2language1"),
                    "city1",
                    Sets.<List<Object>>newHashSet(Lists.<Object>newArrayList("uni2", 3, "city0")),
                    Sets.<List<Object>>newHashSet()
            );

            assertThat(actualRow, equalTo(expectedRow));

            actualRow = result.next();

            // TODO remove
            System.out.println(actualRow.toString());

            expectedRow = new LdbcQuery1Result(
                    3L,
                    "last0",
                    1,
                    3L,
                    3L,
                    "gender3",
                    "browser3",
                    "ip3",
                    Sets.newHashSet("friend3email1", "friend3email2"),
                    Sets.newHashSet("friend3language0"),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("company0", 1, "country0"))
            );

            assertThat(actualRow, equalTo(expectedRow));

            actualRow = result.next();

            // TODO remove
            System.out.println(actualRow.toString());

            expectedRow = new LdbcQuery1Result(
                    1L,
                    "last1",
                    1,
                    1L,
                    1L,
                    "gender1",
                    "browser1",
                    "ip1",
                    Sets.newHashSet("friend1email1", "friend1email2"),
                    Sets.newHashSet("friend1language0"),
                    "city0",
                    Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("uni0", 0, "city1")),
                    Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("company0", 0, "country0"))
            );

            assertThat(actualRow, equalTo(expectedRow));

            actualRow = result.next();

            // TODO remove
            System.out.println(actualRow.toString());

            expectedRow = new LdbcQuery1Result(
                    11L,
                    "last11",
                    2,
                    11L,
                    11L,
                    "gender11",
                    "browser11",
                    "ip11",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city0",
                    Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("uni1", 1, "city0"), Lists.<Object>newArrayList("uni2", 2, "city0")),
                    Lists.<List<Object>>newArrayList()
            );

            assertThat(actualRow, equalTo(expectedRow));

            actualRow = result.next();

            // TODO remove
            System.out.println(actualRow.toString());

            expectedRow = new LdbcQuery1Result(
                    31L,
                    "last31",
                    2,
                    31L,
                    31L,
                    "gender31",
                    "browser31",
                    "ip31",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList()
            );

            assertThat(actualRow, equalTo(expectedRow));

            assertThat(result.hasNext(), is(false));

            personId = 0;
            personUri = null;
            friendName = "name1";
            limit = 1;
            operation = new LdbcQuery1(personId, personUri, friendName, limit);

            result = neo4jQuery1Impl(connection, operation);

            actualRow = result.next();

            // TODO remove
            System.out.println(actualRow.toString());

            expectedRow = new LdbcQuery1Result(
                    21L,
                    "last21",
                    2,
                    21L,
                    21L,
                    "gender21",
                    "browser21",
                    "ip21",
                    Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(),
                    "city1",
                    Lists.<List<Object>>newArrayList(),
                    Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("company1", 2, "country1"))
            );

            assertThat(actualRow, equalTo(expectedRow));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query2ShouldReturnExpectedResult() throws Exception {

        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query2GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            LdbcQuery2 operation;
            Iterator<LdbcQuery2Result> results;
            LdbcQuery2Result actualRow;
            LdbcQuery2Result expectedRow;

            long personId;
            String personUri;
            Date maxDate;
            int limit;

            personId = 1;
            personUri = null;
            maxDate = new Date(3);
            limit = 10;
            // TODO comment out to hide
            System.out.println(String.format("Params: id:%s, date:%s, limit:%s", personId, maxDate.getTime(), limit));
            operation = new LdbcQuery2(personId, personUri, maxDate, limit);
            results = neo4jQuery2Impl(connection, operation);
            assertThat(results.hasNext(), is(true));

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3",
                    2,
                    "[f3Post2] content",
                    3);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3",
                    3,
                    "[f3Post3] content",
                    3);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3",
                    16,
                    "[f3Comment1] content",
                    3);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2",
                    6,
                    "[f2Post2] content",
                    2);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2",
                    7,
                    "[f2Post3] content",
                    2);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2",
                    13,
                    "[f2Comment1] content",
                    2);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            assertThat(results.hasNext(), is(false));

            personId = 1;
            personUri = null;
            maxDate = new Date(4);
            limit = 5;
            // TODO comment out to hide
            System.out.println(String.format("Params: id:%s, date:%s, limit:%s", personId, maxDate.getTime(), limit));
            operation = new LdbcQuery2(personId, personUri, maxDate, limit);
            results = neo4jQuery2Impl(connection, operation);
            assertThat(results.hasNext(), is(true));

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3",
                    1,
                    "[f3Post1] content",
                    4);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    4,
                    "f4",
                    "last4",
                    4,
                    "[f4Post1] content",
                    4);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2",
                    5,
                    "[f2Post1] content",
                    4);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    2,
                    "f2",
                    "last2",
                    14,
                    "[f2Comment2] content",
                    4);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            expectedRow = new LdbcQuery2Result(
                    3,
                    "f3",
                    "last3",
                    2,
                    "[f3Post2] content",
                    3);
            actualRow = results.next();
            assertThat(actualRow, equalTo(expectedRow));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query3ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query3GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            LdbcQuery3 operation;
            Iterator<LdbcQuery3Result> results;
            LdbcQuery3Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            int expectedXCount;
            int expectedYCount;
            int expectedCount;

            long personId;
            String personUri;
            String countryXName;
            String countryYName;
            Date startDate;
            int durationDays;
            int limit;

            personId = 1;
            personUri = null;
            countryXName = "country1";
            countryYName = "country2";
            Calendar c = Calendar.getInstance();
            c.clear();
            c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
            startDate = c.getTime();
            durationDays = 2;
            limit = 10;
            operation = new LdbcQuery3(personId, personUri, countryXName, countryYName, startDate, durationDays, limit);

            results = neo4jQuery3Impl(connection, operation);

            assertThat(results.hasNext(), is(true));

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "f2";
            expectedPersonLastName = "last2";
            expectedXCount = 1;
            expectedYCount = 1;
            expectedCount = 2;
            assertThat(
                    actualResult,
                    equalTo(new LdbcQuery3Result(expectedPersonId, expectedPersonFirstName, expectedPersonLastName, expectedXCount, expectedYCount, expectedCount)));

            actualResult = results.next();
            expectedPersonId = 6l;
            expectedPersonFirstName = "ff6";
            expectedPersonLastName = "last6";
            expectedXCount = 1;
            expectedYCount = 1;
            expectedCount = 2;
            assertThat(
                    actualResult,
                    equalTo(new LdbcQuery3Result(expectedPersonId, expectedPersonFirstName, expectedPersonLastName, expectedXCount, expectedYCount, expectedCount)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query4ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query4GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            Calendar c = Calendar.getInstance();
            Date startDate;
            int durationDays;
            int limit;
            LdbcQuery4 operation;

            Iterator<LdbcQuery4Result> results;
            LdbcQuery4Result actualResult;
            String expectedTagName;
            int expectedTagCount;

            personId = 1;
            personUri = null;
            c.clear();
            c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
            startDate = c.getTime();
            durationDays = 2;
            limit = 10;
            operation = new LdbcQuery4(personId, personUri, startDate, durationDays, limit);

            results = neo4jQuery4Impl(connection, operation);

            expectedTagName = "tag2";
            expectedTagCount = 3;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery4Result(expectedTagName, expectedTagCount)));

            expectedTagName = "tag3";
            expectedTagCount = 2;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery4Result(expectedTagName, expectedTagCount)));

            expectedTagName = "tag5";
            expectedTagCount = 1;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery4Result(expectedTagName, expectedTagCount)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query5ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query5GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            Calendar c = Calendar.getInstance();
            Date joinDate;
            int limit;
            LdbcQuery5 operation;

            Iterator<LdbcQuery5Result> results;
            LdbcQuery5Result actualResult;
            String expectedForumTitle;
            int expectedPostCount;

            personId = 1;
            personUri = null;
            c.clear();
            c.set(2000, Calendar.JANUARY, 2);
            joinDate = c.getTime();
            limit = 4;
            operation = new LdbcQuery5(personId, personUri, joinDate, limit);

            // TODO remove
            System.out.println(joinDate.getTime());

            results = neo4jQuery5Impl(connection, operation);

            actualResult = results.next();
            expectedForumTitle = "forum1";
            expectedPostCount = 1;
            assertThat(actualResult, equalTo(new LdbcQuery5Result(expectedForumTitle, expectedPostCount)));

            actualResult = results.next();
            expectedForumTitle = "forum3";
            expectedPostCount = 1;
            assertThat(actualResult, equalTo(new LdbcQuery5Result(expectedForumTitle, expectedPostCount)));

            actualResult = results.next();
            expectedForumTitle = "forum2";
            expectedPostCount = 0;
            assertThat(actualResult, equalTo(new LdbcQuery5Result(expectedForumTitle, expectedPostCount)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query6ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query6GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            String tagName;
            int limit;
            LdbcQuery6 operation;
            Iterator<LdbcQuery6Result> results;
            String expectedTagName;
            int expectedPostCount;
            LdbcQuery6Result actualResult;

            personId = 1;
            personUri = null;
            tagName = "tag3";
            limit = 4;
            operation = new LdbcQuery6(personId, personUri, tagName, limit);
            results = neo4jQuery6Impl(connection, operation);

            expectedTagName = "tag2";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery6Result(expectedTagName, expectedPostCount)));

            expectedTagName = "tag5";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery6Result(expectedTagName, expectedPostCount)));

            expectedTagName = "tag1";
            expectedPostCount = 1;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery6Result(expectedTagName, expectedPostCount)));

            assertThat(results.hasNext(), is(false));

            personId = 1;
            personUri = null;
            tagName = "tag1";
            limit = 10;
            operation = new LdbcQuery6(personId, personUri, tagName, limit);
            results = neo4jQuery6Impl(connection, operation);

            expectedTagName = "tag2";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery6Result(expectedTagName, expectedPostCount)));

            expectedTagName = "tag4";
            expectedPostCount = 2;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery6Result(expectedTagName, expectedPostCount)));

            expectedTagName = "tag3";
            expectedPostCount = 1;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery6Result(expectedTagName, expectedPostCount)));

            expectedTagName = "tag5";
            expectedPostCount = 1;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery6Result(expectedTagName, expectedPostCount)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query7ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query7GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            int limit;
            LdbcQuery7 operation;
            Calendar c = Calendar.getInstance();

            Iterator<LdbcQuery7Result> results;
            LdbcQuery7Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            long expectedLikeCreationDate;
            long expectedCommentOrPostId;
            String expectedCommentOrPostContent;
            int expectedMinutesLatency;
            boolean expectedIsNew;

            personId = 1;
            personUri = null;
            limit = 7;
            operation = new LdbcQuery7(personId, personUri, limit);
            results = neo4jQuery7Impl(connection, operation);

            expectedPersonId = 8l;
            expectedPersonFirstName = "s8";
            expectedPersonLastName = "last8";
            c.clear();
            c.set(2000, Calendar.JANUARY, 1, 0, 10, 0);
            expectedLikeCreationDate = c.getTimeInMillis();
            expectedCommentOrPostId = 2l;
            expectedCommentOrPostContent = "person1post2";
            expectedMinutesLatency = 8;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery7Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedLikeCreationDate,
                            expectedCommentOrPostId,
                            expectedCommentOrPostContent,
                            expectedMinutesLatency,
                            expectedIsNew))
            );

            expectedPersonId = 7l;
            expectedPersonFirstName = "s7";
            expectedPersonLastName = "last7";
            c.clear();
            c.set(2000, Calendar.JANUARY, 1, 0, 6, 0);
            expectedLikeCreationDate = c.getTimeInMillis();
            expectedCommentOrPostId = 5l;
            expectedCommentOrPostContent = "person1comment1";
            expectedMinutesLatency = 1;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery7Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedLikeCreationDate,
                            expectedCommentOrPostId,
                            expectedCommentOrPostContent,
                            expectedMinutesLatency,
                            expectedIsNew))
            );

            expectedPersonId = 2l;
            expectedPersonFirstName = "f2";
            expectedPersonLastName = "last2";
            c.clear();
            c.set(2000, Calendar.JANUARY, 1, 0, 5, 0);
            expectedLikeCreationDate = c.getTimeInMillis();
            expectedCommentOrPostId = 1l;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 4;
            expectedIsNew = false;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery7Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedLikeCreationDate,
                            expectedCommentOrPostId,
                            expectedCommentOrPostContent,
                            expectedMinutesLatency,
                            expectedIsNew))
            );

            expectedPersonId = 4l;
            expectedPersonFirstName = "f4";
            expectedPersonLastName = "last4";
            c.clear();
            c.set(2000, Calendar.JANUARY, 1, 0, 5, 0);
            expectedLikeCreationDate = c.getTimeInMillis();
            expectedCommentOrPostId = 3l;
            expectedCommentOrPostContent = "person1post3";
            expectedMinutesLatency = 2;
            expectedIsNew = false;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery7Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedLikeCreationDate,
                            expectedCommentOrPostId,
                            expectedCommentOrPostContent,
                            expectedMinutesLatency,
                            expectedIsNew))
            );

            expectedPersonId = 6l;
            expectedPersonFirstName = "ff6";
            expectedPersonLastName = "last6";
            c.clear();
            c.set(2000, Calendar.JANUARY, 1, 0, 4, 0);
            expectedLikeCreationDate = c.getTimeInMillis();
            expectedCommentOrPostId = 1l;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 3;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery7Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedLikeCreationDate,
                            expectedCommentOrPostId,
                            expectedCommentOrPostContent,
                            expectedMinutesLatency,
                            expectedIsNew))
            );

            expectedPersonId = 1l;
            expectedPersonFirstName = "person1";
            expectedPersonLastName = "last1";
            c.clear();
            c.set(2000, Calendar.JANUARY, 1, 0, 1, 0);
            expectedLikeCreationDate = c.getTimeInMillis();
            expectedCommentOrPostId = 1l;
            expectedCommentOrPostContent = "person1post1";
            expectedMinutesLatency = 0;
            expectedIsNew = true;
            actualResult = results.next();
            assertThat(actualResult, equalTo(new LdbcQuery7Result(
                            expectedPersonId,
                            expectedPersonFirstName,
                            expectedPersonLastName,
                            expectedLikeCreationDate,
                            expectedCommentOrPostId,
                            expectedCommentOrPostContent,
                            expectedMinutesLatency,
                            expectedIsNew))
            );

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query8ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query8GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            // TODO rewrite queries and tests
            assertThat(true, is(false));
            long personId;
            String personUri;
            int limit;
            LdbcQuery8 operation;

            Iterator<LdbcQuery8Result> results;
            LdbcQuery8Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            long expectedCommentId;
            long expectedCommentDate;
            String expectedCommentContent;

            personId = 0;
            personUri = null;
            limit = 7;
            operation = new LdbcQuery8(personId, personUri, limit);
            results = neo4jQuery8Impl(connection, operation);

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two";
            expectedCommentDate = 9l;
            expectedCommentId = 131l;
            expectedCommentContent = "C131";
            assertThat(actualResult, equalTo(new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent)));

            actualResult = results.next();
            expectedPersonId = 3l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "three";
            expectedCommentDate = 6l;
            expectedCommentId = 12l;
            expectedCommentContent = "C12";
            assertThat(actualResult, equalTo(new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent)));

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one";
            expectedCommentDate = 5l;
            expectedCommentId = 2111l;
            expectedCommentContent = "C2111";
            assertThat(actualResult, equalTo(new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent)));

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one";
            expectedCommentDate = 4l;
            expectedCommentId = 111l;
            expectedCommentContent = "C111";
            assertThat(actualResult, equalTo(new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent)));

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two";
            expectedCommentDate = 4l;
            expectedCommentId = 112l;
            expectedCommentContent = "C112";
            assertThat(actualResult, equalTo(new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent)));

            actualResult = results.next();
            expectedPersonId = 3l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "three";
            expectedCommentDate = 3l;
            expectedCommentId = 11l;
            expectedCommentContent = "C11";
            assertThat(actualResult, equalTo(new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent)));

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two";
            expectedCommentDate = 2l;
            expectedCommentId = 211l;
            expectedCommentContent = "C211";
            assertThat(actualResult, equalTo(new LdbcQuery8Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentDate,
                    expectedCommentId,
                    expectedCommentContent)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query9ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query9GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            long latestDateAsMilli;
            Date latestDate;
            int limit;
            LdbcQuery9 operation;

            Iterator<LdbcQuery9Result> results;
            LdbcQuery9Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            long expectedCommentOrPostId;
            String expectedCommentOrPostContent;
            long expectedCommentOrPostCreationDate;

            personId = 0;
            personUri = null;
            latestDateAsMilli = 12;
            latestDate = new Date(latestDateAsMilli);
            limit = 10;
            operation = new LdbcQuery9(personId, personUri, latestDate, limit);
            results = neo4jQuery9Impl(connection, operation);

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one";
            expectedCommentOrPostId = 11l;
            expectedCommentOrPostContent = "P11";
            expectedCommentOrPostCreationDate = 11l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            actualResult = results.next();
            expectedPersonId = 4l;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "four";
            expectedCommentOrPostId = 1211l;
            expectedCommentOrPostContent = "C1211";
            expectedCommentOrPostCreationDate = 10l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one";
            expectedCommentOrPostId = 21111l;
            expectedCommentOrPostContent = "C21111";
            expectedCommentOrPostCreationDate = 9l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two";
            expectedCommentOrPostId = 2111l;
            expectedCommentOrPostContent = "C2111";
            expectedCommentOrPostCreationDate = 8l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one";
            expectedCommentOrPostId = 211l;
            expectedCommentOrPostContent = "C211";
            expectedCommentOrPostCreationDate = 7l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two";
            expectedCommentOrPostId = 21l;
            expectedCommentOrPostContent = "P21";
            expectedCommentOrPostCreationDate = 6l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one";
            expectedCommentOrPostId = 12l;
            expectedCommentOrPostContent = "P12";
            expectedCommentOrPostCreationDate = 4l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "two";
            expectedCommentOrPostId = 311l;
            expectedCommentOrPostContent = "C311";
            expectedCommentOrPostCreationDate = 4l;
            assertThat(actualResult, equalTo(new LdbcQuery9Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommentOrPostId,
                    expectedCommentOrPostContent,
                    expectedCommentOrPostCreationDate)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query10ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query10GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            int month;
            int limit;
            LdbcQuery10 operation;

            Iterator<LdbcQuery10Result> results;
            LdbcQuery10Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            int expectedCommonInterestScore;
            String expectedPersonGender;
            String expectedPersonCityName;

            personId = 0;
            personUri = null;
            month = 1;
            limit = 7;
            operation = new LdbcQuery10(personId, personUri, month, limit);
            results = neo4jQuery10Impl(connection, operation);

            actualResult = results.next();
            expectedPersonId = 22l;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "two two";
            expectedCommonInterestScore = 0;
            expectedPersonGender = "male";
            expectedPersonCityName = "city0";
            assertThat(actualResult, equalTo(new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            )));

            actualResult = results.next();
            expectedPersonId = 11l;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "one one";
            expectedCommonInterestScore = -1;
            expectedPersonGender = "female";
            expectedPersonCityName = "city1";
            assertThat(actualResult, equalTo(new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            )));

            actualResult = results.next();
            expectedPersonId = 12l;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "one two";
            expectedCommonInterestScore = -1;
            expectedPersonGender = "male";
            expectedPersonCityName = "city0";
            assertThat(actualResult, equalTo(new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            )));

            actualResult = results.next();
            expectedPersonId = 21l;
            expectedPersonFirstName = "friendfriend";
            expectedPersonLastName = "two one";
            expectedCommonInterestScore = -1;
            expectedPersonGender = "male";
            expectedPersonCityName = "city0";
            assertThat(actualResult, equalTo(new LdbcQuery10Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedCommonInterestScore,
                    expectedPersonGender,
                    expectedPersonCityName
            )));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query11ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query11GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            String countryName;
            int startedBeforeWorkFromYear;
            int limit;
            LdbcQuery11 operation;

            Iterator<LdbcQuery11Result> results;
            LdbcQuery11Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            String expectedOrganizationName;
            int expectedWorkFromYear;

            personId = 0;
            personUri = null;
            countryName = "country0";
            startedBeforeWorkFromYear = 5;
            limit = 4;
            operation = new LdbcQuery11(personId, personUri, countryName, startedBeforeWorkFromYear, limit);
            results = neo4jQuery11Impl(connection, operation);

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "friend";
            expectedPersonLastName = "one";
            expectedOrganizationName = "company zero";
            expectedWorkFromYear = 2;
            assertThat(actualResult, equalTo(new LdbcQuery11Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedOrganizationName,
                    expectedWorkFromYear)));

            actualResult = results.next();
            expectedPersonId = 11l;
            expectedPersonFirstName = "friend friend";
            expectedPersonLastName = "one one";
            expectedOrganizationName = "company zero";
            expectedWorkFromYear = 3;
            assertThat(actualResult, equalTo(new LdbcQuery11Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedOrganizationName,
                    expectedWorkFromYear)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query12ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query12GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId;
            String personUri;
            String tagClassName;
            int limit;
            LdbcQuery12 operation;

            Iterator<LdbcQuery12Result> results;
            LdbcQuery12Result actualResult;
            long expectedPersonId;
            String expectedPersonFirstName;
            String expectedPersonLastName;
            Iterable<String> expectedTagNames;
            int expectedReplyCount;

            personId = 0;
            personUri = null;
            tagClassName = "1";
            limit = 6;
            operation = new LdbcQuery12(personId, personUri, tagClassName, limit);
            results = neo4jQuery12Impl(connection, operation);

            actualResult = results.next();
            expectedPersonId = 1l;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "1";
            expectedTagNames = Sets.newHashSet("tag111", "tag112", "tag12111");
            expectedReplyCount = 4;
            assertThat(actualResult, equalTo(new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount)));

            actualResult = results.next();
            expectedPersonId = 2l;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "2";
            expectedTagNames = Sets.newHashSet("tag111", "tag112");
            expectedReplyCount = 2;
            assertThat(actualResult, equalTo(new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount)));

            actualResult = results.next();
            expectedPersonId = 3l;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "3";
            expectedTagNames = Sets.newHashSet("tag112", "tag11");
            expectedReplyCount = 2;
            assertThat(actualResult, equalTo(new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount)));

            actualResult = results.next();
            expectedPersonId = 4l;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "4";
            expectedTagNames = Sets.newHashSet();
            expectedReplyCount = 0;
            assertThat(actualResult, equalTo(new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount)));

            actualResult = results.next();
            expectedPersonId = 5l;
            expectedPersonFirstName = "f";
            expectedPersonLastName = "5";
            expectedTagNames = Sets.newHashSet();
            expectedReplyCount = 0;
            assertThat(actualResult, equalTo(new LdbcQuery12Result(
                    expectedPersonId,
                    expectedPersonFirstName,
                    expectedPersonLastName,
                    expectedTagNames,
                    expectedReplyCount)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query13ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query13GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId1;
            String person1Uri;
            long personId2;
            String person2Uri;
            LdbcQuery13 operation;

            Iterator<LdbcQuery13Result> results;
            LdbcQuery13Result actualResult;
            int expectedShortestPathLength;

            personId1 = 0;
            person1Uri = null;
            personId2 = 5;
            person2Uri = null;
            operation = new LdbcQuery13(personId1, person1Uri, personId2, person2Uri);
            results = neo4jQuery13Impl(connection, operation);

            actualResult = results.next();
            expectedShortestPathLength = 5;
            assertThat(actualResult, equalTo(new LdbcQuery13Result(expectedShortestPathLength)));

            assertThat(results.hasNext(), is(false));

            personId1 = 7;
            person1Uri = null;
            personId2 = 3;
            person2Uri = null;
            operation = new LdbcQuery13(personId1, person1Uri, personId2, person2Uri);
            results = neo4jQuery13Impl(connection, operation);

            actualResult = results.next();
            expectedShortestPathLength = 3;
            assertThat(actualResult, equalTo(new LdbcQuery13Result(expectedShortestPathLength)));

            assertThat(results.hasNext(), is(false));

            personId1 = 1;
            person1Uri = null;
            personId2 = 2;
            person2Uri = null;
            operation = new LdbcQuery13(personId1, person1Uri, personId2, person2Uri);
            results = neo4jQuery13Impl(connection, operation);

            actualResult = results.next();
            expectedShortestPathLength = 1;
            assertThat(actualResult, equalTo(new LdbcQuery13Result(expectedShortestPathLength)));

            assertThat(results.hasNext(), is(false));

            personId1 = 1;
            person1Uri = null;
            personId2 = 1;
            person2Uri = null;
            operation = new LdbcQuery13(personId1, person1Uri, personId2, person2Uri);
            results = neo4jQuery13Impl(connection, operation);

            actualResult = results.next();
            expectedShortestPathLength = 0;
            assertThat(actualResult, equalTo(new LdbcQuery13Result(expectedShortestPathLength)));

            assertThat(results.hasNext(), is(false));

            personId1 = 1;
            person1Uri = null;
            personId2 = 8;
            person2Uri = null;
            operation = new LdbcQuery13(personId1, person1Uri, personId2, person2Uri);
            results = neo4jQuery13Impl(connection, operation);

            actualResult = results.next();
            expectedShortestPathLength = -1;
            assertThat(actualResult, equalTo(new LdbcQuery13Result(expectedShortestPathLength)));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }

    @Test
    public void query14ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query14GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {

            // TODO test when start person equals end person
            // TODO test when there is no path
            assertThat(true, is(false));

            long personId1;
            String person1Uri;
            long personId2;
            String person2Uri;
            LdbcQuery14 operation;

            Iterator<LdbcQuery14Result> results;
            LdbcQuery14Result actualResult;
            Collection<Long> expectedPathNodeIds;
            double expectedWeight;

            personId1 = 0;
            person1Uri = null;
            personId2 = 5;
            person2Uri = null;
            operation = new LdbcQuery14(personId1, person1Uri, personId2, person2Uri);
            results = neo4jQuery14Impl(connection, operation);

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList(0l, 1l, 7l, 4l, 8l, 5l);
            expectedWeight = 5.5;
            assertThat(actualResult, equalTo(new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            )));

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList(0l, 1l, 7l, 4l, 6l, 5l);
            expectedWeight = 4.5;
            assertThat(actualResult, equalTo(new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            )));

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList(0l, 1l, 2l, 4l, 8l, 5l);
            expectedWeight = 4.0;
            assertThat(actualResult, equalTo(new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            )));

            actualResult = results.next();
            expectedPathNodeIds = Lists.newArrayList(0l, 1l, 2l, 4l, 6l, 5l);
            expectedWeight = 3.0;
            assertThat(actualResult, equalTo(new LdbcQuery14Result(
                    expectedPathNodeIds,
                    expectedWeight
            )));

            assertThat(results.hasNext(), is(false));
        } finally {
            closeConnection(connection);
        }
    }
}
