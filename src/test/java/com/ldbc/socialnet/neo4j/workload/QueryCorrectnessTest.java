package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.kernel.impl.util.FileUtils;

import java.io.File;
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
        FileUtils.deleteRecursively(new File(dbDir));
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 1;
            String personUri = null;
            Calendar c = Calendar.getInstance();
            c.clear();
            c.set(2013, Calendar.SEPTEMBER, 7, 0, 0, 0);
            Date maxDate = c.getTime();
            int limit = 4;

            LdbcQuery2 operation = new LdbcQuery2(personId, personUri, maxDate, limit);

            Iterator<LdbcQuery2Result> result = neo4jQuery2Impl(connection, operation);

            LdbcQuery2Result row;

            // 3 jacob hansson 3 [jake3] tjena 1378504800000
            row = result.next();
            assertThat(row.personId(), is(3L));
            assertThat(row.personFirstName(), is("jacob"));
            assertThat(row.personLastName(), is("hansson"));
            assertThat(row.postOrCommentId(), is(3L));
            assertThat(row.postOrCommentContent(), is("[jake3] tjena"));
            assertThat(row.postOrCommentCreationDate(), is(1378504800000L));

            // 2 aiya thorpe 5 [aiya1] kia ora 1378418400000
            row = result.next();
            assertThat(row.personId(), is(2L));
            assertThat(row.personFirstName(), is("aiya"));
            assertThat(row.personLastName(), is("thorpe"));
            assertThat(row.postOrCommentId(), is(5L));
            assertThat(row.postOrCommentContent(), is("[aiya1] kia ora"));
            assertThat(row.postOrCommentCreationDate(), is(1378418400000L));

            // 3 jacob hansson 2 [jake2] hej 1378335600000
            row = result.next();
            assertThat(row.personId(), is(3L));
            assertThat(row.personFirstName(), is("jacob"));
            assertThat(row.personLastName(), is("hansson"));
            assertThat(row.postOrCommentId(), is(2L));
            assertThat(row.postOrCommentContent(), is("[jake2] hej"));
            assertThat(row.postOrCommentCreationDate(), is(1378335600000L));

            // 3 jacob hansson 1 [jake1] hello 1378332060000
            row = result.next();
            assertThat(row.personId(), is(3L));
            assertThat(row.personFirstName(), is("jacob"));
            assertThat(row.personLastName(), is("hansson"));
            assertThat(row.postOrCommentId(), is(1L));
            assertThat(row.postOrCommentContent(), is("[jake1] hello"));
            assertThat(row.postOrCommentCreationDate(), is(1378332060000L));

            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query3ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 1;
            String personUri = null;
            String countryXName = "new zealand";
            String countryYName = "sweden";
            Calendar c = Calendar.getInstance();
            c.clear();
            c.set(2013, Calendar.SEPTEMBER, 4);
            Date startDate = c.getTime();
            int durationDays = 4;
            int limit = 2;

            LdbcQuery3 operation = new LdbcQuery3(personId, personUri, countryXName, countryYName, startDate, durationDays, limit);
            Iterator<LdbcQuery3Result> result = neo4jQuery3Impl(connection, operation);

            // Has at least 1 result
            assertThat(result.hasNext(), is(true));

            LdbcQuery3Result firstRow = result.next();

            assertThat(firstRow.personId(), is(3L));
            assertThat(firstRow.personFirstName(), is("jacob"));
            assertThat(firstRow.personLastName(), is("hansson"));
            assertThat(firstRow.xCount(), is(1L));
            assertThat(firstRow.yCount(), is(2L));
            assertThat(firstRow.count(), is(3L));

            // Has at least 2 results
            assertThat(result.hasNext(), is(true));

            LdbcQuery3Result secondRow = result.next();

            assertThat(secondRow.personId(), is(2L));
            assertThat(secondRow.personFirstName(), is("aiya"));
            assertThat(secondRow.personLastName(), is("thorpe"));
            assertThat(secondRow.xCount(), is(1L));
            assertThat(secondRow.yCount(), is(1L));
            assertThat(secondRow.count(), is(2L));

            // Has exactly 2 results, no more
            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query4ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 1;
            String personUri = null;
            Calendar c = Calendar.getInstance();
            c.clear();
            c.set(2013, Calendar.SEPTEMBER, 5, 23, 59, 0);
            Date startDate = c.getTime();
            int durationDays = 2;
            int limit = 10;

            LdbcQuery4 operation = new LdbcQuery4(personId, personUri, startDate, durationDays, limit);
            Iterator<LdbcQuery4Result> result = neo4jQuery4Impl(connection, operation);

            int expectedRowCount = 5;
            int actualRowCount = 0;

            assertThat(result.next(), equalTo(new LdbcQuery4Result("pie", 3)));
            assertThat(result.next(), equalTo(new LdbcQuery4Result("lol", 2)));
            actualRowCount = 2;

            Map<String, Integer> validTags = new HashMap<>();
            validTags.put("cake", 1);
            validTags.put("yolo", 1);
            validTags.put("wtf", 1);

            while (result.hasNext()) {
                LdbcQuery4Result row = result.next();
                assertThat(row.tagCount(), is(validTags.get(row.tagName())));
                actualRowCount++;
            }
            assertThat(actualRowCount, is(expectedRowCount));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query5ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 1;
            String personUri = null;
            Calendar c = Calendar.getInstance();
            c.clear();
            c.set(2013, Calendar.JANUARY, 8);
            Date joinDate = c.getTime();

            int limit = 3;

            LdbcQuery5 operation = new LdbcQuery5(personId, personUri, joinDate, limit);
            Iterator<LdbcQuery5Result> result = neo4jQuery5Impl(connection, operation);

            assertThat(result.hasNext(), is(true));
            assertThat(result.next(), equalTo(new LdbcQuery5Result("everything cakes and pies", 5)));
            assertThat(result.hasNext(), is(true));
            assertThat(result.next(), equalTo(new LdbcQuery5Result("boats are not submarines", 2)));
            assertThat(result.hasNext(), is(true));
            assertThat(result.next(), equalTo(new LdbcQuery5Result("kiwis sheep and bungy jumping", 1)));
            assertThat(result.hasNext(), is(false));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query6ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
        CONNECTION connection = openConnection(dbDir);
        try {
            long personId = 1;
            String personUri = null;
            String tagName = "lol";
            int limit = 10;

            LdbcQuery6 operation = new LdbcQuery6(personId, personUri, tagName, limit);
            Iterator<LdbcQuery6Result> result = neo4jQuery6Impl(connection, operation);

            int expectedRowCount = 3;
            int actualRowCount = 0;

            Map<String, Long> validTagCounts = new HashMap<>();
            validTagCounts.put("wtf", 2L);
            validTagCounts.put("pie", 2L);
            validTagCounts.put("cake", 1L);

            while (result.hasNext()) {
                LdbcQuery6Result row = result.next();
                String tag = row.tagName();
                assertThat(validTagCounts.containsKey(tag), is(true));
                long tagCount = row.tagCount();
                assertThat(validTagCounts.get(tag), equalTo(tagCount));
                actualRowCount++;
            }
            assertThat(expectedRowCount, equalTo(actualRowCount));
        } finally {
            closeConnection(connection);
            FileUtils.deleteRecursively(new File(dbDir));
        }
    }

    @Test
    public void query7ShouldReturnExpectedResult() throws Exception {
        String dbDir = testFolder.newFolder().getAbsolutePath();
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
        FileUtils.deleteRecursively(new File(dbDir));
        TestGraph.createDbFromQueryGraphMaker(new TestGraph.Query1GraphMaker(), dbDir);
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
