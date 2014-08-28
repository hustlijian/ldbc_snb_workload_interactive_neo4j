package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.kernel.impl.util.FileUtils;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
            FileUtils.deleteRecursively(new File(dbDir));
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
            FileUtils.deleteRecursively(new File(dbDir));
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
            FileUtils.deleteRecursively(new File(dbDir));
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
            FileUtils.deleteRecursively(new File(dbDir));
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
            FileUtils.deleteRecursively(new File(dbDir));
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
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query7ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query7GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 1;
            String personUri = null;
            int limit = 5;

            LdbcQuery7 operation = new LdbcQuery7(personId, personUri, limit);
            Iterator<LdbcQuery7Result> result = neo4jQuery7Impl(connection, operation);

            LdbcQuery7Result row;

            row = result.next();
            assertThat(row.personId(), is(2L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("one"));
            assertThat(row.likeCreationDate(), is(5L));
            assertThat(row.isNew(), is(false));
            assertThat(row.commentOrPostId(), is(1L));
            assertThat(row.commentOrPostContent(), equalTo("p1"));
            // TODO this will fail because the current test creates a seconds delay, rather than minutes, graph or expected result need to change
            assertThat(row.minutesLatency(), equalTo(5));

            row = result.next();
            assertThat(row.personId(), is(4L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("three"));
            assertThat(row.likeCreationDate(), is(5L));
            assertThat(row.isNew(), is(false));
            assertThat(row.commentOrPostId(), is(1L));
            assertThat(row.commentOrPostContent(), equalTo("p1"));
            // TODO this will fail because the current test creates a seconds delay, rather than minutes, graph or expected result need to change
            assertThat(row.minutesLatency(), equalTo(5));

            row = result.next();
            assertThat(row.personId(), is(4L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("three"));
            assertThat(row.likeCreationDate(), is(4L));
            assertThat(row.isNew(), is(false));
            assertThat(row.commentOrPostId(), is(2L));
            assertThat(row.commentOrPostContent(), equalTo("p2"));
            // TODO this will fail because the current test creates a seconds delay, rather than minutes, graph or expected result need to change
            assertThat(row.minutesLatency(), equalTo(4));

            row = result.next();
            assertThat(row.personId(), is(6L));
            assertThat(row.personFirstName(), is("friendfriend"));
            assertThat(row.personLastName(), is("two"));
            assertThat(row.likeCreationDate(), is(3L));
            assertThat(row.isNew(), is(true));
            assertThat(row.commentOrPostId(), is(1L));
            assertThat(row.commentOrPostContent(), equalTo("p1"));
            // TODO this will fail because the current test creates a seconds delay, rather than minutes, graph or expected result need to change
            assertThat(row.minutesLatency(), equalTo(3));

            row = result.next();
            assertThat(row.personId(), is(6L));
            assertThat(row.personFirstName(), is("friendfriend"));
            assertThat(row.personLastName(), is("two"));
            assertThat(row.likeCreationDate(), is(2L));
            assertThat(row.isNew(), is(true));
            assertThat(row.commentOrPostId(), is(2L));
            assertThat(row.commentOrPostContent(), equalTo("p2"));
            // TODO this will fail because the current test creates a seconds delay, rather than minutes, graph or expected result need to change
            assertThat(row.minutesLatency(), equalTo(2));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query8ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query8GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 0;
            String personUri = null;
            int limit = 7;

            LdbcQuery8 operation = new LdbcQuery8(personId, personUri, limit);
            Iterator<LdbcQuery8Result> result = neo4jQuery8Impl(connection, operation);

            LdbcQuery8Result row;

            row = result.next();
            assertThat(row.personId(), is(2L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("two"));
            assertThat(row.commentCreationDate(), is(8L));
            assertThat(row.commentId(), is(13L));
            assertThat(row.commentContent(), is("C13"));

            row = result.next();
            assertThat(row.personId(), is(3L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("three"));
            assertThat(row.commentCreationDate(), is(6L));
            assertThat(row.commentId(), is(12L));
            assertThat(row.commentContent(), is("C12"));

            row = result.next();
            assertThat(row.personId(), is(1L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("one"));
            assertThat(row.commentCreationDate(), is(5L));
            assertThat(row.commentId(), is(2111L));
            assertThat(row.commentContent(), is("C2111"));

            row = result.next();
            assertThat(row.personId(), is(1L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("one"));
            assertThat(row.commentCreationDate(), is(4L));
            assertThat(row.commentId(), is(111L));
            assertThat(row.commentContent(), is("C111"));

            row = result.next();
            assertThat(row.personId(), is(2L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("two"));
            assertThat(row.commentCreationDate(), is(4L));
            assertThat(row.commentId(), is(112L));
            assertThat(row.commentContent(), is("C112"));

            row = result.next();
            assertThat(row.personId(), is(3L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("three"));
            assertThat(row.commentCreationDate(), is(3L));
            assertThat(row.commentId(), is(11L));
            assertThat(row.commentContent(), is("C11"));
            row = result.next();
            assertThat(row.personId(), is(2L));
            assertThat(row.personFirstName(), is("friend"));
            assertThat(row.personLastName(), is("two"));
            assertThat(row.commentCreationDate(), is(2L));
            assertThat(row.commentId(), is(211L));
            assertThat(row.commentContent(), is("C211"));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query9ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query9GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 0;
            String personUri = null;
            long latestDateAsMilli = 12;
            Date latestDate = new Date(latestDateAsMilli);
            int limit = 7;

            LdbcQuery9 operation = new LdbcQuery9(personId, personUri, latestDate, limit);
            Iterator<LdbcQuery9Result> result = neo4jQuery9Impl(connection, operation);

            LdbcQuery9Result row;

            row = result.next();
            assertThat(row.commentOrPostId(), equalTo(11L));
            assertThat(row.commentOrPostCreationDate(), is(11L));
            assertThat(row.commentOrPostContent(), equalTo("P11"));

            row = result.next();
            assertThat(row.commentOrPostId(), equalTo(1211L));
            assertThat(row.commentOrPostCreationDate(), is(10L));
            assertThat(row.commentOrPostContent(), equalTo("C1211"));

            row = result.next();
            assertThat(row.commentOrPostId(), equalTo(2111L));
            assertThat(row.commentOrPostCreationDate(), is(8L));
            assertThat(row.commentOrPostContent(), equalTo("C2111"));

            row = result.next();
            assertThat(row.commentOrPostId(), equalTo(211L));
            assertThat(row.commentOrPostCreationDate(), is(7L));
            assertThat(row.commentOrPostContent(), equalTo("C211"));

            row = result.next();
            assertThat(row.commentOrPostId(), equalTo(21L));
            assertThat(row.commentOrPostCreationDate(), is(6L));
            assertThat(row.commentOrPostContent(), equalTo("P21"));

            row = result.next();
            assertThat(row.commentOrPostId(), equalTo(12L));
            assertThat(row.commentOrPostCreationDate(), is(4L));
            assertThat(row.commentOrPostContent(), equalTo("P12"));

            row = result.next();
            assertThat(row.commentOrPostId(), equalTo(311L));
            assertThat(row.commentOrPostCreationDate(), is(4L));
            assertThat(row.commentOrPostContent(), equalTo("C311"));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query10ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query10GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 0;
            String personUri = null;
            int month1 = 2;
            int month2 = 3;
            int limit = 7;

            LdbcQuery10 operation = new LdbcQuery10(personId, personUri, month1, month2, limit);
            Iterator<LdbcQuery10Result> result = neo4jQuery10Impl(connection, operation);

            LdbcQuery10Result row;

            row = result.next();
            assertThat(row.personId(), equalTo(11L));
            assertThat(row.personFirstName(), equalTo("friendfriend"));
            assertThat(row.personLastName(), equalTo("one one"));
            assertThat(row.commonInterestScore(), equalTo(1 / 3D));
            assertThat(row.personGender(), equalTo("female"));
            assertThat(row.personCityName(), equalTo("city1"));

            row = result.next();
            assertThat(row.personId(), equalTo(21L));
            assertThat(row.personFirstName(), equalTo("friendfriend"));
            assertThat(row.personLastName(), equalTo("two one"));
            assertThat(row.commonInterestScore(), equalTo(1 / 3D));
            assertThat(row.personGender(), equalTo("male"));
            assertThat(row.personCityName(), equalTo("city0"));

            row = result.next();
            assertThat(row.personId(), equalTo(12L));
            assertThat(row.personFirstName(), equalTo("friendfriend"));
            assertThat(row.personLastName(), equalTo("one two"));
            assertThat(row.commonInterestScore(), equalTo(0D));
            assertThat(row.personGender(), equalTo("male"));
            assertThat(row.personCityName(), equalTo("city0"));

            row = result.next();
            assertThat(row.personId(), equalTo(22L));
            assertThat(row.personFirstName(), equalTo("friendfriend"));
            assertThat(row.personLastName(), equalTo("two two"));
            assertThat(row.commonInterestScore(), equalTo(0D));
            assertThat(row.personGender(), equalTo("male"));
            assertThat(row.personCityName(), equalTo("city0"));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query11ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query11GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 0;
            String personUri = null;
            String countryName = "country0";
            int maxWorkFromYear = 4;
            int limit = 3;

            LdbcQuery11 operation = new LdbcQuery11(personId, personUri, countryName, maxWorkFromYear, limit);
            Iterator<LdbcQuery11Result> result = neo4jQuery11Impl(connection, operation);

            LdbcQuery11Result row;

            row = result.next();
            assertThat(row.personId(), equalTo(1L));
            assertThat(row.personFirstName(), equalTo("friend"));
            assertThat(row.personLastName(), equalTo("one"));
            assertThat(row.organizationName(), equalTo("company zero"));
            assertThat(row.organizationWorkFromYear(), equalTo(2));

            row = result.next();
            assertThat(row.personId(), equalTo(11L));
            assertThat(row.personFirstName(), equalTo("friend friend"));
            assertThat(row.personLastName(), equalTo("one one"));
            assertThat(row.organizationName(), equalTo("company zero"));
            assertThat(row.organizationWorkFromYear(), equalTo(3));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query12ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query12GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 0;
            String personUri = null;
            String tagClassName = "1";
            int limit = 4;

            // long personId, String personUri, String tagClassName, int limit
            LdbcQuery12 operation = new LdbcQuery12(personId, personUri, tagClassName, limit);
            Iterator<LdbcQuery12Result> result = neo4jQuery12Impl(connection, operation);

            LdbcQuery12Result row;

            row = result.next();
            assertThat(row.personId(), equalTo(1L));
            assertThat(row.personFirstName(), equalTo("f"));
            assertThat(row.personLastName(), equalTo("1"));
            assertThat(Lists.newArrayList(row.tagNames()).size(), equalTo(3));
            assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.newHashSet("tag111", "tag112", "tag12111")));
            assertThat(row.replyCount(), equalTo(4));

            row = result.next();
            assertThat(row.personId(), equalTo(2L));
            assertThat(row.personFirstName(), equalTo("f"));
            assertThat(row.personLastName(), equalTo("2"));
            assertThat(Lists.newArrayList(row.tagNames()).size(), equalTo(2));
            assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.newHashSet("tag111", "tag112")));
            assertThat(row.replyCount(), equalTo(2));

            row = result.next();
            assertThat(row.personId(), equalTo(3L));
            assertThat(row.personFirstName(), equalTo("f"));
            assertThat(row.personLastName(), equalTo("3"));
            assertThat(Lists.newArrayList(row.tagNames()).size(), equalTo(2));
            assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.newHashSet("tag112", "tag11")));
            assertThat(row.replyCount(), equalTo(2));

            row = result.next();
            assertThat(row.personId(), equalTo(4L));
            assertThat(row.personFirstName(), equalTo("f"));
            assertThat(row.personLastName(), equalTo("4"));
            assertThat(Lists.newArrayList(row.tagNames()).size(), equalTo(0));
            assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.<String>newHashSet()));
            assertThat(row.replyCount(), equalTo(0));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query13ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query13GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId1 = 0;
            String person1Uri = null;
            long personId2 = 5;
            String person2Uri = null;

            LdbcQuery13 operation = new LdbcQuery13(personId1, person1Uri, personId2, person2Uri);
            Iterator<LdbcQuery13Result> result = neo4jQuery13Impl(connection, operation);

            LdbcQuery13Result row;

            row = result.next();
            assertThat(row.shortestPathLength(), equalTo(5));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query14ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query14GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId1 = 0;
            String person1Uri = null;
            long personId2 = 1;
            String person2Uri = null;

            LdbcQuery14 operation = new LdbcQuery14(personId1, person1Uri, personId2, person2Uri);
            Iterator<LdbcQuery14Result> result = neo4jQuery14Impl(connection, operation);

            LdbcQuery14Result row;

            // TODO this test will totally fail, needs to be remade from scratch

            row = result.next();
            assertThat(row.pathWeight(), equalTo(1.0));
//        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Post", 0L), new PathNode("Comment", 0L), new PathNode("Person", 1L))));

            row = result.next();
            assertThat(row.pathWeight(), equalTo(0.5));
//        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Comment", 5L), new PathNode("Comment", 6L), new PathNode("Person", 1L))));

            row = result.next();
            assertThat(row.pathWeight(), equalTo(0.5));
//        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Comment", 1L), new PathNode("Comment", 0L), new PathNode("Person", 1L))));


            row = result.next();
            assertThat(row.pathWeight(), equalTo(0.5));
//        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Comment", 1L), new PathNode("Comment", 2L), new PathNode("Person", 1L))));

            row = result.next();
            assertThat(row.pathWeight(), equalTo(2.0));
//        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Post", 0L), new PathNode("Comment", 0L), new PathNode("Comment", 1L), new PathNode("Comment", 2L), new PathNode("Person", 1L))));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }
}
