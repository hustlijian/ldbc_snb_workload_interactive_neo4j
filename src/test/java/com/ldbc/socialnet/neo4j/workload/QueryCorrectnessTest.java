package com.ldbc.socialnet.neo4j.workload;

import com.google.common.collect.Sets;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.*;
import com.ldbc.socialnet.workload.neo4j.interactive.*;
import com.ldbc.socialnet.workload.neo4j.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public abstract class QueryCorrectnessTest {
    public static String dbDir = "tempDb";
    public static GraphDatabaseService db = null;
    public static ExecutionEngine engine = null;

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

    public abstract Neo4jQuery1 neo4jQuery1Impl();

    public abstract Neo4jQuery2 neo4jQuery2Impl();

    public abstract Neo4jQuery3 neo4jQuery3Impl();

    public abstract Neo4jQuery4 neo4jQuery4Impl();

    public abstract Neo4jQuery5 neo4jQuery5Impl();

    public abstract Neo4jQuery6 neo4jQuery6Impl();

    public abstract Neo4jQuery7 neo4jQuery7Impl();

    public abstract Neo4jQuery8 neo4jQuery8Impl();

    public abstract Neo4jQuery9 neo4jQuery9Impl();

    public abstract Neo4jQuery10 neo4jQuery10Impl();

    public abstract Neo4jQuery11 neo4jQuery11Impl();

    public abstract Neo4jQuery12 neo4jQuery12Impl();

    public abstract Neo4jQuery13 neo4jQuery13Impl();

    public abstract Neo4jQuery14 neo4jQuery14Impl();

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
        Map dbRunConfig = Utils.loadConfig(TestUtils.getResource("/neo4j_run_dev.properties").getAbsolutePath());
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbDir).setConfig(dbRunConfig).newGraphDatabase();
        engine = new ExecutionEngine(db);
    }

    @Test
    public void query1ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query1GraphMaker());

        long personId = 0;
        String friendName = "name0";
        int limit = 6;
        LdbcQuery1 operation1 = new LdbcQuery1(personId, friendName, limit);
        Neo4jQuery1 query1 = neo4jQuery1Impl();

        // TODO uncomment to print query
        System.out.println(operation1.toString() + "\n" + query1.description() + "\n");

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery1Result> result = query1.execute(db, engine, operation1);

            LdbcQuery1Result row;

            row = result.next();
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

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
    }

    @Test
    public void query2ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query2GraphMaker());

        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.SEPTEMBER, 7, 0, 0, 0);
        Date maxDate = c.getTime();
        int limit = 4;

        LdbcQuery2 operation = new LdbcQuery2(personId, maxDate, limit);
        Neo4jQuery2 query = neo4jQuery2Impl();

        // TODO uncomment to print query
        System.out.println(operation.toString() + "\n" + query.description() + "\n");

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery2Result> result = query.execute(db, engine, operation);

            LdbcQuery2Result row = null;

            /*
            3   jacob   hansson 3   [jake3] tjena   1378504800000
            2   aiya    thorpe  5   [aiya1] kia ora 1378418400000
            3   jacob   hansson 2   [jake2] hej 1378335600000
            3   jacob   hansson 1   [jake1] hello   1378332060000
            */

            // TODO * add post.id to nodes when bulk loading

            // TODO * add calendar.clear() everywhere it is used

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

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
    }

    @Test
    public void query3ShouldReturnExpectedResult() throws IOException {
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

        LdbcQuery3 operation3 = new LdbcQuery3(personId, countryX, countryY, endDate, durationMillis);
        Neo4jQuery3 query3 = neo4jQuery3Impl();

        // TODO uncomment to print query
        System.out.println(operation3.toString() + "\n" + query3.description() + "\n");

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery3Result> result = query3.execute(db, engine, operation3);

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
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
    }

    @Test
    public void query4ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query4GraphMaker());

        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.SEPTEMBER, 7, 23, 59, 0);
        Date endDate = c.getTime();
        long durationDays = 2;
        long durationMillis = TimeUnit.MILLISECONDS.convert(durationDays, TimeUnit.DAYS);

        LdbcQuery4 operation4 = new LdbcQuery4(personId, endDate, durationMillis);
        Neo4jQuery4 query4 = neo4jQuery4Impl();

        // TODO uncomment to print query
        System.out.println(operation4.toString() + "\n" + query4.description() + "\n");

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery4Result> result = query4.execute(db, engine, operation4);

            int expectedRowCount = 5;
            int actualRowCount = 0;

            assertThat(result.next(), equalTo(new LdbcQuery4Result("pie", 3)));
            assertThat(result.next(), equalTo(new LdbcQuery4Result("lol", 2)));
            actualRowCount = 2;

            Map<String, Integer> validTags = new HashMap<String, Integer>();
            validTags.put("cake", 1);
            validTags.put("yolo", 1);
            validTags.put("wtf", 1);

            while (result.hasNext()) {
                LdbcQuery4Result row = result.next();
                assertThat(row.tagCount(), is(validTags.get(row.tagName())));
                actualRowCount++;
            }
            assertThat(actualRowCount, is(expectedRowCount));

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
    }

    @Test
    public void query5ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query5GraphMaker());

        long personId = 1;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.JANUARY, 8);
        Date joinDate = c.getTime();

        LdbcQuery5 operation5 = new LdbcQuery5(personId, joinDate);
        Neo4jQuery5 query5 = neo4jQuery5Impl();

        // TODO uncomment to print query
        System.out.println(operation5.toString() + "\n" + query5.description() + "\n");

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery5Result> result = query5.execute(db, engine, operation5);

            assertThat(result.hasNext(), is(true));
            assertThat(result.next(), equalTo(new LdbcQuery5Result("everything cakes and pies", 5)));
            assertThat(result.hasNext(), is(true));
            assertThat(result.next(), equalTo(new LdbcQuery5Result("boats are not submarines", 2)));
            assertThat(result.hasNext(), is(true));
            assertThat(result.next(), equalTo(new LdbcQuery5Result("kiwis sheep and bungy jumping", 1)));
            assertThat(result.hasNext(), is(false));

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
    }

    @Test
    public void query6ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query6GraphMaker());

        long personId = 1;
        String tagName = "lol";
        int limit = 10;

        LdbcQuery6 operation6 = new LdbcQuery6(personId, tagName, limit);
        Neo4jQuery6 query6 = neo4jQuery6Impl();

        // TODO uncomment to print query
        System.out.println(operation6.toString() + "\n" + query6.description() + "\n");

        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery6Result> result = query6.execute(db, engine, operation6);

            int expectedRowCount = 3;
            int actualRowCount = 0;

            Map<String, Long> validTagCounts = new HashMap<String, Long>();
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

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(false));
    }

    @Test
    public void query7ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query7GraphMaker());

        long personId = 1;
        int limit = 5;

        LdbcQuery7 operation7 = new LdbcQuery7(personId, limit);
        Neo4jQuery7 query7 = neo4jQuery7Impl();

        // TODO uncomment to print query
        System.out.println(operation7.toString() + "\n" + query7.description() + "\n");

        String stackTrace = null;
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery7Result> result = query7.execute(db, engine, operation7);

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

            tx.success();
        } catch (Exception e) {
            stackTrace = ConcurrentErrorReporter.stackTraceToString(e);
            exceptionThrown = true;
        }
        assertThat(stackTrace, exceptionThrown, is(false));
    }

    @Test
    public void query8ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query8GraphMaker());

        long personId = 0;
        int limit = 7;

        LdbcQuery8 operation8 = new LdbcQuery8(personId, limit);
        Neo4jQuery8 query8 = neo4jQuery8Impl();

        // TODO uncomment to print query
        System.out.println(operation8.toString() + "\n" + query8.description() + "\n");

        String stackTrace = null;
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery8Result> result = query8.execute(db, engine, operation8);

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

            tx.success();
        } catch (Exception e) {
            stackTrace = ConcurrentErrorReporter.stackTraceToString(e);
            exceptionThrown = true;
        }
        assertThat(stackTrace, exceptionThrown, is(false));
    }

    @Test
    public void query9ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query9GraphMaker());

        long personId = 0;
        long latestDateAsMilli = 12;
        int limit = 7;

        LdbcQuery9 operation9 = new LdbcQuery9(personId, latestDateAsMilli, limit);
        Neo4jQuery9 query9 = neo4jQuery9Impl();

        // TODO uncomment to print query
        System.out.println(operation9.toString() + "\n" + query9.description() + "\n");

        String stackTrace = null;
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery9Result> result = query9.execute(db, engine, operation9);

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

            tx.success();
        } catch (Exception e) {
            stackTrace = ConcurrentErrorReporter.stackTraceToString(e);
            exceptionThrown = true;
        }
        assertThat(stackTrace, exceptionThrown, is(false));
    }

    @Test
    public void query10ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query10GraphMaker());

        long personId = 0;
        int horoscopeMonth1 = 2;
        int horoscopeMonth2 = 3;
        int limit = 7;

        LdbcQuery10 operation10 = new LdbcQuery10(personId, horoscopeMonth1, horoscopeMonth2, limit);
        Neo4jQuery10 query10 = neo4jQuery10Impl();

        // TODO uncomment to print query
        System.out.println("\n" + operation10.toString() + "\n" + query10.description() + "\n");

        String stackTrace = null;
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery10Result> result = query10.execute(db, engine, operation10);

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

            tx.success();
        } catch (Exception e) {
            stackTrace = ConcurrentErrorReporter.stackTraceToString(e);
            exceptionThrown = true;
        }
        assertThat(stackTrace, exceptionThrown, is(false));
    }

    @Test
    public void query11ShouldReturnExpectedResult() throws IOException {
        createDb(new TestGraph.Query11GraphMaker());

        long personId = 0;
        String countryName = "country0";
        int maxWorkFromYear = 4;
        int limit = 3;

        LdbcQuery11 operation11 = new LdbcQuery11(personId, countryName, maxWorkFromYear, limit);
        Neo4jQuery11 query11 = neo4jQuery11Impl();

        // TODO uncomment to print query
        System.out.println("\n" + operation11.toString() + "\n" + query11.description() + "\n");

        String stackTrace = null;
        boolean exceptionThrown = false;
        try (Transaction tx = db.beginTx()) {
            Iterator<LdbcQuery11Result> result = query11.execute(db, engine, operation11);

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

            tx.success();
        } catch (Exception e) {
            stackTrace = ConcurrentErrorReporter.stackTraceToString(e);
            exceptionThrown = true;
        }
        assertThat(stackTrace, exceptionThrown, is(false));
    }

    @Ignore
    @Test
    public void query12ShouldReturnExpectedResult() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void query13ShouldReturnExpectedResult() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void query14ShouldReturnExpectedResult() {
        assertThat(true, is(false));
    }
}
