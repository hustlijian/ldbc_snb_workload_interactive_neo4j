package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.DbException;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery14Result.PathNode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public abstract class QueryCorrectnessTest {
    private static String dbDir = "tempDb";
    private static GraphDatabaseService db = null;
    private static ExecutionEngine engine = null;

    @Before
    public void init() throws IOException {
        FileUtils.deleteRecursively(new File(dbDir));
    }

    @After
    public void closeDb() throws IOException {
        db.shutdown();
        FileUtils.deleteRecursively(new File(dbDir));
    }

    public static void buildGraph(ExecutionEngine engine, GraphDatabaseService db, String createQuery, Map<String, Object> queryParams) {
        try (Transaction tx = db.beginTx()) {
            engine.execute(createQuery, queryParams);
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        try (Transaction tx = db.beginTx()) {
            for (String createIndexQuery : TestGraph.createIndexQueries()) {
                engine.execute(createIndexQuery);
            }
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public abstract Iterator<LdbcQuery1Result> neo4jQuery1Impl(String path, LdbcQuery1 operation) throws DbException;

    public abstract Iterator<LdbcQuery2Result> neo4jQuery2Impl(String path, LdbcQuery2 operation) throws DbException;

    public abstract Iterator<LdbcQuery3Result> neo4jQuery3Impl(String path, LdbcQuery3 operation) throws DbException;

    public abstract Iterator<LdbcQuery4Result> neo4jQuery4Impl(String path, LdbcQuery4 operation) throws DbException;

    public abstract Iterator<LdbcQuery5Result> neo4jQuery5Impl(String path, LdbcQuery5 operation) throws DbException;

    public abstract Iterator<LdbcQuery6Result> neo4jQuery6Impl(String path, LdbcQuery6 operation) throws DbException;

    public abstract Iterator<LdbcQuery7Result> neo4jQuery7Impl(String path, LdbcQuery7 operation) throws DbException;

    public abstract Iterator<LdbcQuery8Result> neo4jQuery8Impl(String path, LdbcQuery8 operation) throws DbException;

    public abstract Iterator<LdbcQuery9Result> neo4jQuery9Impl(String path, LdbcQuery9 operation) throws DbException;

    public abstract Iterator<LdbcQuery10Result> neo4jQuery10Impl(String path, LdbcQuery10 operation) throws DbException;

    public abstract Iterator<LdbcQuery11Result> neo4jQuery11Impl(String path, LdbcQuery11 operation) throws DbException;

    public abstract Iterator<LdbcQuery12Result> neo4jQuery12Impl(String path, LdbcQuery12 operation) throws DbException;

    public abstract Iterator<LdbcQuery13Result> neo4jQuery13Impl(String path, LdbcQuery13 operation) throws DbException;

    public abstract Iterator<LdbcQuery14Result> neo4jQuery14Impl(String path, LdbcQuery14 operation) throws DbException;

    public void createDb(TestGraph.QueryGraphMaker queryGraphMaker) throws IOException {
        // TODO uncomment to print CREATE
        System.out.println();
        System.out.println(MapUtils.prettyPrint(queryGraphMaker.params()));
        System.out.println(queryGraphMaker.graph());

        Map dbImportConfig = Utils.loadConfig(TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath());
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbDir).setConfig(dbImportConfig).newGraphDatabase();
        engine = new ExecutionEngine(db);
        buildGraph(engine, db, queryGraphMaker.graph(), queryGraphMaker.params());
        db.shutdown();
    }

    @Test
    public void query1ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query1GraphMaker());

        long personId = 0;
        String friendName = "name0";
        int limit = 6;
        LdbcQuery1 operation = new LdbcQuery1(personId, friendName, limit);

        Iterator<LdbcQuery1Result> result = neo4jQuery1Impl(dbDir, operation);

        LdbcQuery1Result row;

        row = result.next();

        // TODO remove
        System.out.println(row.toString());

        assertThat(row.friendId(), equalTo(1L));
        assertThat(row.friendLastName(), equalTo("last1"));
        assertThat(row.distanceFromPerson(), equalTo(1));
        assertThat(row.friendBirthday(), equalTo(1L));
        assertThat(row.friendCreationDate(), equalTo(1L));
        assertThat(row.friendGender(), equalTo("gender1"));
        assertThat(row.friendBrowserUsed(), equalTo("browser1"));
        assertThat(row.friendLocationIp(), equalTo("ip1"));
        assertThat(row.friendEmails(), equalTo((Set) Sets.newHashSet("friend1email1", "friend1email2")));
        assertThat(row.friendLanguages(), equalTo((Set) Sets.newHashSet("friend1language0")));
        assertThat(row.friendCityName(), equalTo("city0"));
        assertThat(row.friendUniversities(), equalTo((Set) Sets.newHashSet("uni0,city1,0")));
        assertThat(row.friendCompanies(), equalTo((Set) Sets.newHashSet("company0,country0,0")));

        row = result.next();

        // TODO remove
        System.out.println(row.toString());

        assertThat(row.friendId(), equalTo(2L));
        assertThat(row.friendLastName(), equalTo("last2"));
        assertThat(row.distanceFromPerson(), equalTo(1));
        assertThat(row.friendBirthday(), equalTo(2L));
        assertThat(row.friendCreationDate(), equalTo(2L));
        assertThat(row.friendGender(), equalTo("gender2"));
        assertThat(row.friendBrowserUsed(), equalTo("browser2"));
        assertThat(row.friendLocationIp(), equalTo("ip2"));
        assertThat(row.friendEmails(), equalTo((Set) Sets.newHashSet()));
        assertThat(row.friendLanguages(), equalTo((Set) Sets.newHashSet("friend2language0", "friend2language1")));
        assertThat(row.friendCityName(), equalTo("city1"));
        assertThat(row.friendUniversities(), equalTo((Set) Sets.newHashSet("uni2,city0,3")));
        assertThat(row.friendCompanies(), equalTo((Set) Sets.newHashSet()));

        row = result.next();

        // TODO remove
        System.out.println(row.toString());

        assertThat(row.friendId(), equalTo(3L));
        assertThat(row.friendLastName(), equalTo("last3"));
        assertThat(row.distanceFromPerson(), equalTo(1));
        assertThat(row.friendBirthday(), equalTo(3L));
        assertThat(row.friendCreationDate(), equalTo(3L));
        assertThat(row.friendGender(), equalTo("gender3"));
        assertThat(row.friendBrowserUsed(), equalTo("browser3"));
        assertThat(row.friendLocationIp(), equalTo("ip3"));
        assertThat(row.friendEmails(), equalTo((Set) Sets.newHashSet("friend3email1", "friend3email2")));
        assertThat(row.friendLanguages(), equalTo((Set) Sets.newHashSet("friend3language0")));
        assertThat(row.friendCityName(), equalTo("city1"));
        assertThat(row.friendUniversities(), equalTo((Set) Sets.newHashSet()));
        assertThat(row.friendCompanies(), equalTo((Set) Sets.newHashSet("company0,country0,1")));


        row = result.next();

        // TODO remove
        System.out.println(row.toString());

        assertThat(row.friendId(), equalTo(11L));
        assertThat(row.friendLastName(), equalTo("last11"));
        assertThat(row.distanceFromPerson(), equalTo(2));
        assertThat(row.friendBirthday(), equalTo(11L));
        assertThat(row.friendCreationDate(), equalTo(11L));
        assertThat(row.friendGender(), equalTo("gender11"));
        assertThat(row.friendBrowserUsed(), equalTo("browser11"));
        assertThat(row.friendLocationIp(), equalTo("ip11"));
        assertThat(row.friendEmails(), equalTo((Set) Sets.newHashSet()));
        assertThat(row.friendLanguages(), equalTo((Set) Sets.newHashSet()));
        assertThat(row.friendCityName(), equalTo("city0"));
        assertThat(row.friendUniversities(), equalTo((Set) Sets.newHashSet("uni1,city0,1", "uni2,city0,2")));
        assertThat(row.friendCompanies(), equalTo((Set) Sets.newHashSet()));

        row = result.next();

        // TODO remove
        System.out.println(row.toString());

        assertThat(row.friendId(), equalTo(31L));
        assertThat(row.friendLastName(), equalTo("last31"));
        assertThat(row.distanceFromPerson(), equalTo(2));
        assertThat(row.friendBirthday(), equalTo(31L));
        assertThat(row.friendCreationDate(), equalTo(31L));
        assertThat(row.friendGender(), equalTo("gender31"));
        assertThat(row.friendBrowserUsed(), equalTo("browser31"));
        assertThat(row.friendLocationIp(), equalTo("ip31"));
        assertThat(row.friendEmails(), equalTo((Set) Sets.newHashSet()));
        assertThat(row.friendLanguages(), equalTo((Set) Sets.newHashSet()));
        assertThat(row.friendCityName(), equalTo("city1"));
        assertThat(row.friendUniversities(), equalTo((Set) Sets.newHashSet()));
        assertThat(row.friendCompanies(), equalTo((Set) Sets.newHashSet()));
    }

    @Test
    public void query2ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query2GraphMaker());

        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.SEPTEMBER, 7, 0, 0, 0);
        Date maxDate = c.getTime();
        int limit = 4;

        LdbcQuery2 operation = new LdbcQuery2(personId, maxDate, limit);

        Iterator<LdbcQuery2Result> result = neo4jQuery2Impl(dbDir, operation);

        LdbcQuery2Result row;

        // 3 jacob hansson 3 [jake3] tjena 1378504800000
        row = result.next();
        assertThat(row.personId(), is(3L));
        assertThat(row.personFirstName(), is("jacob"));
        assertThat(row.personLastName(), is("hansson"));
        assertThat(row.postId(), is(3L));
        assertThat(row.postContent(), is("[jake3] tjena"));
        assertThat(row.postDate(), is(1378504800000L));

        // 2 aiya thorpe 5 [aiya1] kia ora 1378418400000
        row = result.next();
        assertThat(row.personId(), is(2L));
        assertThat(row.personFirstName(), is("aiya"));
        assertThat(row.personLastName(), is("thorpe"));
        assertThat(row.postId(), is(5L));
        assertThat(row.postContent(), is("[aiya1] kia ora"));
        assertThat(row.postDate(), is(1378418400000L));

        // 3 jacob hansson 2 [jake2] hej 1378335600000
        row = result.next();
        assertThat(row.personId(), is(3L));
        assertThat(row.personFirstName(), is("jacob"));
        assertThat(row.personLastName(), is("hansson"));
        assertThat(row.postId(), is(2L));
        assertThat(row.postContent(), is("[jake2] hej"));
        assertThat(row.postDate(), is(1378335600000L));

        // 3 jacob hansson 1 [jake1] hello 1378332060000
        row = result.next();
        assertThat(row.personId(), is(3L));
        assertThat(row.personFirstName(), is("jacob"));
        assertThat(row.personLastName(), is("hansson"));
        assertThat(row.postId(), is(1L));
        assertThat(row.postContent(), is("[jake1] hello"));
        assertThat(row.postDate(), is(1378332060000L));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query3ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query3GraphMaker());

        long personId = 1;
        String countryX = "new zealand";
        String countryY = "sweden";
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.SEPTEMBER, 8);
        Date endDate = c.getTime();
        int durationDays = 4;
        long durationMillis = TimeUnit.MILLISECONDS.convert(durationDays, TimeUnit.DAYS);
        int limit = 2;

        LdbcQuery3 operation = new LdbcQuery3(personId, countryX, countryY, endDate, durationMillis, limit);
        Iterator<LdbcQuery3Result> result = neo4jQuery3Impl(dbDir, operation);

        // Has at least 1 result
        assertThat(result.hasNext(), is(true));

        LdbcQuery3Result firstRow = result.next();

        assertThat(firstRow.friendName(), is("jacob hansson"));
        assertThat(firstRow.xCount(), is(1L));
        assertThat(firstRow.yCount(), is(2L));
        assertThat(firstRow.xyCount(), is(3L));

        // Has at least 2 results
        assertThat(result.hasNext(), is(true));

        LdbcQuery3Result secondRow = result.next();

        assertThat(secondRow.friendName(), is("aiya thorpe"));
        assertThat(secondRow.xCount(), is(1L));
        assertThat(secondRow.yCount(), is(1L));
        assertThat(secondRow.xyCount(), is(2L));

        // Has exactly 2 results, no more
        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query4ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query4GraphMaker());

        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.SEPTEMBER, 7, 23, 59, 0);
        Date endDate = c.getTime();
        long durationDays = 2;
        long durationMillis = TimeUnit.MILLISECONDS.convert(durationDays, TimeUnit.DAYS);
        int limit = 10;

        LdbcQuery4 operation = new LdbcQuery4(personId, endDate, durationMillis, limit);
        Iterator<LdbcQuery4Result> result = neo4jQuery4Impl(dbDir, operation);

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
    }

    @Test
    public void query5ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query5GraphMaker());

        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.JANUARY, 8);
        Date joinDate = c.getTime();

        LdbcQuery5 operation = new LdbcQuery5(personId, joinDate);
        Iterator<LdbcQuery5Result> result = neo4jQuery5Impl(dbDir, operation);

        assertThat(result.hasNext(), is(true));
        assertThat(result.next(), equalTo(new LdbcQuery5Result("everything cakes and pies", 5)));
        assertThat(result.hasNext(), is(true));
        assertThat(result.next(), equalTo(new LdbcQuery5Result("boats are not submarines", 2)));
        assertThat(result.hasNext(), is(true));
        assertThat(result.next(), equalTo(new LdbcQuery5Result("kiwis sheep and bungy jumping", 1)));
        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query6ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query6GraphMaker());

        long personId = 1;
        String tagName = "lol";
        int limit = 10;

        LdbcQuery6 operation = new LdbcQuery6(personId, tagName, limit);
        Iterator<LdbcQuery6Result> result = neo4jQuery6Impl(dbDir, operation);

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
    }

    @Test
    public void query7ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query7GraphMaker());

        long personId = 1;
        int limit = 5;

        LdbcQuery7 operation = new LdbcQuery7(personId, limit);
        Iterator<LdbcQuery7Result> result = neo4jQuery7Impl(dbDir, operation);

        LdbcQuery7Result row;

        row = result.next();
        assertThat(row.personId(), is(2L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("one"));
        assertThat(row.likeCreationDate().getTime(), is(5L));
        assertThat(row.isNew(), is(false));
        assertThat(row.postId(), is(1L));
        assertThat(row.postContent(), equalTo("p1"));
        assertThat(row.milliSecondDelay(), equalTo(5L));

        row = result.next();
        assertThat(row.personId(), is(4L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("three"));
        assertThat(row.likeCreationDate().getTime(), is(5L));
        assertThat(row.isNew(), is(false));
        assertThat(row.postId(), is(1L));
        assertThat(row.postContent(), equalTo("p1"));
        assertThat(row.milliSecondDelay(), equalTo(5L));

        row = result.next();
        assertThat(row.personId(), is(4L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("three"));
        assertThat(row.likeCreationDate().getTime(), is(4L));
        assertThat(row.isNew(), is(false));
        assertThat(row.postId(), is(2L));
        assertThat(row.postContent(), equalTo("p2"));
        assertThat(row.milliSecondDelay(), equalTo(4L));

        row = result.next();
        assertThat(row.personId(), is(6L));
        assertThat(row.personFirstName(), is("friendfriend"));
        assertThat(row.personLastName(), is("two"));
        assertThat(row.likeCreationDate().getTime(), is(3L));
        assertThat(row.isNew(), is(true));
        assertThat(row.postId(), is(1L));
        assertThat(row.postContent(), equalTo("p1"));
        assertThat(row.milliSecondDelay(), equalTo(3L));

        row = result.next();
        assertThat(row.personId(), is(6L));
        assertThat(row.personFirstName(), is("friendfriend"));
        assertThat(row.personLastName(), is("two"));
        assertThat(row.likeCreationDate().getTime(), is(2L));
        assertThat(row.isNew(), is(true));
        assertThat(row.postId(), is(2L));
        assertThat(row.postContent(), equalTo("p2"));
        assertThat(row.milliSecondDelay(), equalTo(2L));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query8ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query8GraphMaker());

        long personId = 0;
        int limit = 7;

        LdbcQuery8 operation = new LdbcQuery8(personId, limit);
        Iterator<LdbcQuery8Result> result = neo4jQuery8Impl(dbDir, operation);

        LdbcQuery8Result row;

        row = result.next();
        assertThat(row.personId(), is(2L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("two"));
        assertThat(row.replyCreationDate(), is(8L));
        assertThat(row.replyId(), is(13L));
        assertThat(row.replyContent(), is("C13"));

        row = result.next();
        assertThat(row.personId(), is(3L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("three"));
        assertThat(row.replyCreationDate(), is(6L));
        assertThat(row.replyId(), is(12L));
        assertThat(row.replyContent(), is("C12"));

        row = result.next();
        assertThat(row.personId(), is(1L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("one"));
        assertThat(row.replyCreationDate(), is(5L));
        assertThat(row.replyId(), is(2111L));
        assertThat(row.replyContent(), is("C2111"));

        row = result.next();
        assertThat(row.personId(), is(1L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("one"));
        assertThat(row.replyCreationDate(), is(4L));
        assertThat(row.replyId(), is(111L));
        assertThat(row.replyContent(), is("C111"));

        row = result.next();
        assertThat(row.personId(), is(2L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("two"));
        assertThat(row.replyCreationDate(), is(4L));
        assertThat(row.replyId(), is(112L));
        assertThat(row.replyContent(), is("C112"));

        row = result.next();
        assertThat(row.personId(), is(3L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("three"));
        assertThat(row.replyCreationDate(), is(3L));
        assertThat(row.replyId(), is(11L));
        assertThat(row.replyContent(), is("C11"));
        row = result.next();
        assertThat(row.personId(), is(2L));
        assertThat(row.personFirstName(), is("friend"));
        assertThat(row.personLastName(), is("two"));
        assertThat(row.replyCreationDate(), is(2L));
        assertThat(row.replyId(), is(211L));
        assertThat(row.replyContent(), is("C211"));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query9ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query9GraphMaker());

        long personId = 0;
        long latestDateAsMilli = 12;
        int limit = 7;

        LdbcQuery9 operation = new LdbcQuery9(personId, latestDateAsMilli, limit);
        Iterator<LdbcQuery9Result> result = neo4jQuery9Impl(dbDir, operation);

        LdbcQuery9Result row;

        row = result.next();
        assertThat(row.getPostOrCommentId(), equalTo(11L));
        assertThat(row.getPostOrCommentCreationDate(), is(11L));
        assertThat(row.getPostOrCommentContent(), equalTo("P11"));

        row = result.next();
        assertThat(row.getPostOrCommentId(), equalTo(1211L));
        assertThat(row.getPostOrCommentCreationDate(), is(10L));
        assertThat(row.getPostOrCommentContent(), equalTo("C1211"));

        row = result.next();
        assertThat(row.getPostOrCommentId(), equalTo(2111L));
        assertThat(row.getPostOrCommentCreationDate(), is(8L));
        assertThat(row.getPostOrCommentContent(), equalTo("C2111"));

        row = result.next();
        assertThat(row.getPostOrCommentId(), equalTo(211L));
        assertThat(row.getPostOrCommentCreationDate(), is(7L));
        assertThat(row.getPostOrCommentContent(), equalTo("C211"));

        row = result.next();
        assertThat(row.getPostOrCommentId(), equalTo(21L));
        assertThat(row.getPostOrCommentCreationDate(), is(6L));
        assertThat(row.getPostOrCommentContent(), equalTo("P21"));

        row = result.next();
        assertThat(row.getPostOrCommentId(), equalTo(12L));
        assertThat(row.getPostOrCommentCreationDate(), is(4L));
        assertThat(row.getPostOrCommentContent(), equalTo("P12"));

        row = result.next();
        assertThat(row.getPostOrCommentId(), equalTo(311L));
        assertThat(row.getPostOrCommentCreationDate(), is(4L));
        assertThat(row.getPostOrCommentContent(), equalTo("C311"));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query10ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query10GraphMaker());

        long personId = 0;
        int horoscopeMonth1 = 2;
        int horoscopeMonth2 = 3;
        int limit = 7;

        LdbcQuery10 operation = new LdbcQuery10(personId, horoscopeMonth1, horoscopeMonth2, limit);
        Iterator<LdbcQuery10Result> result = neo4jQuery10Impl(dbDir, operation);

        LdbcQuery10Result row;

        row = result.next();
        assertThat(row.personId(), equalTo(11L));
        assertThat(row.personFirstName(), equalTo("friendfriend"));
        assertThat(row.personLastName(), equalTo("one one"));
        assertThat(row.commonInterestScore(), equalTo(1 / 3D));
        assertThat(row.gender(), equalTo("female"));
        assertThat(row.personCityName(), equalTo("city1"));

        row = result.next();
        assertThat(row.personId(), equalTo(21L));
        assertThat(row.personFirstName(), equalTo("friendfriend"));
        assertThat(row.personLastName(), equalTo("two one"));
        assertThat(row.commonInterestScore(), equalTo(1 / 3D));
        assertThat(row.gender(), equalTo("male"));
        assertThat(row.personCityName(), equalTo("city0"));

        row = result.next();
        assertThat(row.personId(), equalTo(12L));
        assertThat(row.personFirstName(), equalTo("friendfriend"));
        assertThat(row.personLastName(), equalTo("one two"));
        assertThat(row.commonInterestScore(), equalTo(0D));
        assertThat(row.gender(), equalTo("male"));
        assertThat(row.personCityName(), equalTo("city0"));

        row = result.next();
        assertThat(row.personId(), equalTo(22L));
        assertThat(row.personFirstName(), equalTo("friendfriend"));
        assertThat(row.personLastName(), equalTo("two two"));
        assertThat(row.commonInterestScore(), equalTo(0D));
        assertThat(row.gender(), equalTo("male"));
        assertThat(row.personCityName(), equalTo("city0"));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query11ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query11GraphMaker());

        long personId = 0;
        String countryName = "country0";
        int maxWorkFromYear = 4;
        int limit = 3;

        LdbcQuery11 operation = new LdbcQuery11(personId, countryName, maxWorkFromYear, limit);
        Iterator<LdbcQuery11Result> result = neo4jQuery11Impl(dbDir, operation);

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
    }

    @Test
    public void query12ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query12GraphMaker());

        long personId = 0;
        long tagClassId = 1;
        int limit = 4;

        LdbcQuery12 operation = new LdbcQuery12(personId, tagClassId, limit);
        Iterator<LdbcQuery12Result> result = neo4jQuery12Impl(dbDir, operation);

        LdbcQuery12Result row;

        row = result.next();
        assertThat(row.personId(), equalTo(1L));
        assertThat(row.personFirstName(), equalTo("f"));
        assertThat(row.personLastName(), equalTo("1"));
        assertThat(row.tagNames().size(), equalTo(3));
        assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.newHashSet("tag111", "tag112", "tag12111")));
        assertThat(row.replyCount(), equalTo(4L));

        row = result.next();
        assertThat(row.personId(), equalTo(2L));
        assertThat(row.personFirstName(), equalTo("f"));
        assertThat(row.personLastName(), equalTo("2"));
        assertThat(row.tagNames().size(), equalTo(2));
        assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.newHashSet("tag111", "tag112")));
        assertThat(row.replyCount(), equalTo(2L));

        row = result.next();
        assertThat(row.personId(), equalTo(3L));
        assertThat(row.personFirstName(), equalTo("f"));
        assertThat(row.personLastName(), equalTo("3"));
        assertThat(row.tagNames().size(), equalTo(2));
        assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.newHashSet("tag112", "tag11")));
        assertThat(row.replyCount(), equalTo(2L));

        row = result.next();
        assertThat(row.personId(), equalTo(4L));
        assertThat(row.personFirstName(), equalTo("f"));
        assertThat(row.personLastName(), equalTo("4"));
        assertThat(row.tagNames().size(), equalTo(0));
        assertThat(Sets.newHashSet(row.tagNames()), equalTo(Sets.<String>newHashSet()));
        assertThat(row.replyCount(), equalTo(0L));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query13ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query13GraphMaker());

        long personId1 = 0;
        long personId2 = 5;

        LdbcQuery13 operation = new LdbcQuery13(personId1, personId2);
        Iterator<LdbcQuery13Result> result = neo4jQuery13Impl(dbDir, operation);

        LdbcQuery13Result row;

        row = result.next();
        assertThat(row.shortestPathLength(), equalTo(5));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void query14ShouldReturnExpectedResult() throws IOException, DbException {
        createDb(new TestGraph.Query14GraphMaker());

        long personId1 = 0;
        long personId2 = 1;
        int limit = 5;

        LdbcQuery14 operation = new LdbcQuery14(personId1, personId2, limit);
        Iterator<LdbcQuery14Result> result = neo4jQuery14Impl(dbDir, operation);

        LdbcQuery14Result row;

        row = result.next();
        assertThat(row.weight(), equalTo(1.0));
        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Post", 0L), new PathNode("Comment", 0L), new PathNode("Person", 1L))));

        row = result.next();
        assertThat(row.weight(), equalTo(0.5));
        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Comment", 5L), new PathNode("Comment", 6L), new PathNode("Person", 1L))));

        row = result.next();
        assertThat(row.weight(), equalTo(0.5));
        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Comment", 1L), new PathNode("Comment", 0L), new PathNode("Person", 1L))));


        row = result.next();
        assertThat(row.weight(), equalTo(0.5));
        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Comment", 1L), new PathNode("Comment", 2L), new PathNode("Person", 1L))));

        row = result.next();
        assertThat(row.weight(), equalTo(2.0));
        assertThat(Lists.newArrayList(row.pathNodes()), equalTo(Lists.newArrayList(new PathNode("Person", 0L), new PathNode("Post", 0L), new PathNode("Comment", 0L), new PathNode("Comment", 1L), new PathNode("Comment", 2L), new PathNode("Person", 1L))));

        assertThat(result.hasNext(), is(false));
    }
}
