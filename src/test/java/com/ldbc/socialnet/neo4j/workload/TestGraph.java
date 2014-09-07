package com.ldbc.socialnet.neo4j.workload;

import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple.Tuple2;
import com.ldbc.socialnet.neo4j.TestUtils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.MapUtil;

import java.util.*;

import static com.ldbc.socialnet.workload.neo4j.Domain.*;

public class TestGraph {
    //    static int port = 6362;
    static int port = 6366;

    private static Iterable<String> createIndexQueries() {
        List<String> createIndexQueries = new ArrayList<>();
        for (Tuple2<Label, String> labelAndProperty : labelPropertyPairsToIndex()) {
            createIndexQueries.add("CREATE INDEX ON :" + labelAndProperty._1() + "(" + labelAndProperty._2() + ")");
        }
        return createIndexQueries;
    }

    public static void createDbFromQueryGraphMaker(TestGraph.QueryGraphMaker queryGraphMaker, String path) throws Exception {
        // TODO uncomment to print CREATE
        System.out.println();
        System.out.println(MapUtils.prettyPrint(queryGraphMaker.params()));
        System.out.println(queryGraphMaker.queryString());
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(path)
                .loadPropertiesFromFile(TestUtils.getResource("/neo4j_import_dev.properties").getAbsolutePath())
                .setConfig(Settings.setting("online_backup_enabled", Settings.BOOLEAN, Settings.FALSE), "false")
                .newGraphDatabase();
        ExecutionEngine engine = new ExecutionEngine(db);
        createDbFromCypherQuery(engine, db, queryGraphMaker.queryString(), queryGraphMaker.params());
        db.shutdown();
    }

    public static void createDbFromCypherQuery(ExecutionEngine engine, GraphDatabaseService db, String createQuery, Map<String, Object> queryParams) {
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


    public static interface QueryGraphMaker {
        String queryString();

        Map<String, Object> params();
    }

    public static class Query1GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                   /*
                   * NOTES
                   */
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (person0:" + Nodes.Person + " {person0}),\n"
                    + " (f1:" + Nodes.Person + " {f1}),\n"
                    + " (f2:" + Nodes.Person + " {f2}),\n"
                    + " (f3:" + Nodes.Person + " {f3}),\n"
                    + " (ff11:" + Nodes.Person + " {ff11}),\n"
                    + " (fff111:" + Nodes.Person + " {fff111}),\n"
                    + " (ffff1111:" + Nodes.Person + " {ffff1111}),\n"
                    + " (fffff11111:" + Nodes.Person + " {fffff11111}),\n"
                    + " (ff21:" + Nodes.Person + " {ff21}),\n"
                    + " (fff211:" + Nodes.Person + " {fff211}),\n"
                    + " (ff31:" + Nodes.Person + " {ff31}),\n"
                   /*
                   * Universities
                   */
                    + " (uni0:" + Organisation.Type.University + " {uni0}),\n"
                    + " (uni1:" + Organisation.Type.University + " {uni1}),\n"
                    + " (uni2:" + Organisation.Type.University + " {uni2}),\n"
                   /*
                   * Companies
                   */
                    + " (company0:" + Organisation.Type.Company + " {company0}),\n"
                    + " (company1:" + Organisation.Type.Company + " {company1}),\n"
                   /*
                   * Cities
                   */
                    + " (city0:" + Place.Type.City + " {city0}),\n"
                    + " (city1:" + Place.Type.City + " {city1}),\n"
                   /*
                   * Countries
                   */
                    + " (country0:" + Place.Type.Country + " {country0}),\n"
                    + " (country1:" + Place.Type.Country + " {country1}),\n"
                   /*
                   * RELATIONSHIP
                   */
                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                   * Person-Person
                   */
                    + " (person0)-[:" + Rels.KNOWS + "]->(f1),\n"
                    + " (person0)-[:" + Rels.KNOWS + "]->(f2),\n"
                    + " (person0)-[:" + Rels.KNOWS + "]->(f3),\n"
                    + " (f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                    + " (f2)-[:" + Rels.KNOWS + "]->(ff11),\n"
                    + " (f2)-[:" + Rels.KNOWS + "]->(ff21),\n"
                    + " (f3)-[:" + Rels.KNOWS + "]->(ff31),\n"
                    + " (ff11)-[:" + Rels.KNOWS + "]->(fff111),\n"
                    + " (fff111)-[:" + Rels.KNOWS + "]->(ffff1111),\n"
                    + " (ffff1111)-[:" + Rels.KNOWS + "]->(fffff11111),\n"
                    + " (ff21)-[:" + Rels.KNOWS + "]->(fff211),\n"
                    + " (f3)-[:" + Rels.KNOWS + "]->(f2),\n"
                   /*
                   * Person-City
                   */
                    + " (person0)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (f1)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (ff11)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (fff111)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (ffff1111)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (fffff11111)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (f2)-[:" + Rels.IS_LOCATED_IN + "]->(city1),\n"
                    + " (ff21)-[:" + Rels.IS_LOCATED_IN + "]->(city1),\n"
                    + " (fff211)-[:" + Rels.IS_LOCATED_IN + "]->(city1),\n"
                    + " (f3)-[:" + Rels.IS_LOCATED_IN + "]->(city1),\n"
                    + " (ff31)-[:" + Rels.IS_LOCATED_IN + "]->(city1),\n"
                   /*
                    * City-Country
                    */
                    + " (city0)-[:" + Rels.IS_PART_OF + "]->(country0),\n"
                    + " (city1)-[:" + Rels.IS_PART_OF + "]->(country1),\n"
                   /*
                    * Company-Country
                    */
                    + " (company0)-[:" + Rels.IS_LOCATED_IN + " ]->(country0),\n"
                    + " (company1)-[:" + Rels.IS_LOCATED_IN + " ]->(country1),\n"
                   /*
                    * University-City
                    */
                    + " (uni0)-[:" + Rels.IS_LOCATED_IN + " ]->(city1),\n"
                    + " (uni1)-[:" + Rels.IS_LOCATED_IN + " ]->(city0),\n"
                    + " (uni2)-[:" + Rels.IS_LOCATED_IN + " ]->(city0),\n"
                   /*
                    * Person-University
                    */
                    + " (f1)-[:" + Rels.STUDY_AT + " {f1StudyAtUni0}]->(uni0),\n"
                    + " (ff11)-[:" + Rels.STUDY_AT + " {ff11StudyAtUni1}]->(uni1),\n"
                    + " (ff11)-[:" + Rels.STUDY_AT + " {ff11StudyAtUni2}]->(uni2),\n"
                    + " (f2)-[:" + Rels.STUDY_AT + " {f2StudyAtUni2}]->(uni2),\n"
                   /*
                    * Person-Company
                    */
                    + " (f1)-[:" + Rels.WORKS_AT + " {f1WorkAtCompany0}]->(company0),\n"
                    + " (f3)-[:" + Rels.WORKS_AT + " {f3WorkAtCompany0}]->(company0),\n"
                    + " (ff21)-[:" + Rels.WORKS_AT + " {ff21WorkAtCompany1}]->(company1)\n";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "ff11", TestPersons.ff11(),
                    "fff111", TestPersons.fff111(),
                    "ffff1111", TestPersons.ffff1111(),
                    "fffff11111", TestPersons.fffff11111(),
                    "ff21", TestPersons.ff21(),
                    "fff211", TestPersons.fff211(),
                    "ff31", TestPersons.ff31(),
                    // Universities
                    "uni0", TestUniversities.uni0(),
                    "uni1", TestUniversities.uni1(),
                    "uni2", TestUniversities.uni2(),
                    // Companies
                    "company0", TestCompanies.company0(),
                    "company1", TestCompanies.company1(),
                    // Cities
                    "city0", TestCities.city0(),
                    "city1", TestCities.city1(),
                    // Countries
                    "country0", TestCountries.country0(),
                    "country1", TestCountries.country1(),
                    // WorkAt
                    "f1WorkAtCompany0", TestWorkAt.f1WorkAtCompany0(),
                    "f3WorkAtCompany0", TestWorkAt.f3WorkAtCompany0(),
                    "ff21WorkAtCompany1", TestWorkAt.ff21WorkAtCompany1(),
                    // StudyAt
                    "f1StudyAtUni0", TestStudyAt.f1StudyAtUni0(),
                    "ff11StudyAtUni1", TestStudyAt.ff11StudyAtUni1(),
                    "ff11StudyAtUni2", TestStudyAt.ff11StudyAtUni2(),
                    "f2StudyAtUni2", TestStudyAt.f2StudyAtUni2()
            );
        }

        protected static class TestPersons {
            protected static Map<String, Object> person0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 0L);
                params.put(Person.FIRST_NAME, "person");
                params.put(Person.LAST_NAME, "zero");
                params.put(Person.CREATION_DATE, 0L);
                params.put(Person.BIRTHDAY, 0L);
                params.put(Person.BROWSER_USED, "browser0");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"person0email1", "person0email2"});
                params.put(Person.GENDER, "gender0");
                params.put(Person.LANGUAGES, new String[]{"person0language0", "person0language1"});
                params.put(Person.LOCATION_IP, "ip0");
                return params;
            }

            protected static Map<String, Object> f1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "name0");
                params.put(Person.LAST_NAME, "last1");
                params.put(Person.CREATION_DATE, 1L);
                params.put(Person.BIRTHDAY, 1L);
                params.put(Person.BROWSER_USED, "browser1");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend1email1", "friend1email2"});
                params.put(Person.GENDER, "gender1");
                params.put(Person.LANGUAGES, new String[]{"friend1language0"});
                params.put(Person.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "name0");
                params.put(Person.LAST_NAME, "last0");
                params.put(Person.CREATION_DATE, 2L);
                params.put(Person.BIRTHDAY, 2L);
                params.put(Person.BROWSER_USED, "browser2");
                params.put(Person.EMAIL_ADDRESSES, new String[]{});
                params.put(Person.GENDER, "gender2");
                params.put(Person.LANGUAGES, new String[]{"friend2language0", "friend2language1"});
                params.put(Person.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "name0");
                params.put(Person.LAST_NAME, "last0");
                params.put(Person.CREATION_DATE, 3L);
                params.put(Person.BIRTHDAY, 3L);
                params.put(Person.BROWSER_USED, "browser3");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend3email1", "friend3email2"});
                params.put(Person.GENDER, "gender3");
                params.put(Person.LANGUAGES, new String[]{"friend3language0"});
                params.put(Person.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> ff11() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 11L);
                params.put(Person.FIRST_NAME, "name0");
                params.put(Person.LAST_NAME, "last11");
                params.put(Person.CREATION_DATE, 11L);
                params.put(Person.BIRTHDAY, 11L);
                params.put(Person.BROWSER_USED, "browser11");
                params.put(Person.EMAIL_ADDRESSES, new String[]{});
                params.put(Person.GENDER, "gender11");
                params.put(Person.LANGUAGES, new String[]{});
                params.put(Person.LOCATION_IP, "ip11");
                return params;
            }

            protected static Map<String, Object> fff111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 111L);
                params.put(Person.FIRST_NAME, "name1");
                params.put(Person.LAST_NAME, "last111");
                params.put(Person.CREATION_DATE, 111L);
                params.put(Person.BIRTHDAY, 111L);
                params.put(Person.BROWSER_USED, "browser111");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"fff111email1", "fff111email2", "fff111email3"});
                params.put(Person.GENDER, "gender111");
                params.put(Person.LANGUAGES, new String[]{"fff111language0", "fff111language1", "fff111language2"});
                params.put(Person.LOCATION_IP, "ip111");
                return params;
            }

            protected static Map<String, Object> ffff1111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1111L);
                params.put(Person.FIRST_NAME, "name0");
                params.put(Person.LAST_NAME, "last1111");
                params.put(Person.CREATION_DATE, 1111L);
                params.put(Person.BIRTHDAY, 1111L);
                params.put(Person.BROWSER_USED, "browser1111");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"ffff1111email1"});
                params.put(Person.GENDER, "gender1111");
                params.put(Person.LANGUAGES, new String[]{"ffff1111language0"});
                params.put(Person.LOCATION_IP, "ip1111");
                return params;
            }

            protected static Map<String, Object> fffff11111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 11111L);
                params.put(Person.FIRST_NAME, "name0");
                params.put(Person.LAST_NAME, "last11111");
                params.put(Person.CREATION_DATE, 11111L);
                params.put(Person.BIRTHDAY, 11111L);
                params.put(Person.BROWSER_USED, "browser11111");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"fffff11111email1"});
                params.put(Person.GENDER, "gender11111");
                params.put(Person.LANGUAGES, new String[]{"fffff11111language0"});
                params.put(Person.LOCATION_IP, "ip11111");
                return params;
            }

            protected static Map<String, Object> ff21() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 21L);
                params.put(Person.FIRST_NAME, "name1");
                params.put(Person.LAST_NAME, "last21");
                params.put(Person.CREATION_DATE, 21L);
                params.put(Person.BIRTHDAY, 21L);
                params.put(Person.BROWSER_USED, "browser21");
                params.put(Person.EMAIL_ADDRESSES, new String[]{});
                params.put(Person.GENDER, "gender21");
                params.put(Person.LANGUAGES, new String[]{});
                params.put(Person.LOCATION_IP, "ip21");
                return params;
            }

            protected static Map<String, Object> fff211() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 211L);
                params.put(Person.FIRST_NAME, "name1");
                params.put(Person.LAST_NAME, "last211");
                params.put(Person.CREATION_DATE, 211L);
                params.put(Person.BIRTHDAY, 211L);
                params.put(Person.BROWSER_USED, "browser211");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"fff211email1"});
                params.put(Person.GENDER, "gender211");
                params.put(Person.LANGUAGES, new String[]{});
                params.put(Person.LOCATION_IP, "ip211");
                return params;
            }

            protected static Map<String, Object> ff31() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 31L);
                params.put(Person.FIRST_NAME, "name0");
                params.put(Person.LAST_NAME, "last31");
                params.put(Person.CREATION_DATE, 31L);
                params.put(Person.BIRTHDAY, 31L);
                params.put(Person.BROWSER_USED, "browser31");
                params.put(Person.EMAIL_ADDRESSES, new String[]{});
                params.put(Person.GENDER, "gender31");
                params.put(Person.LANGUAGES, new String[]{});
                params.put(Person.LOCATION_IP, "ip31");
                return params;
            }
        }

        protected static class TestUniversities {
            protected static Map<String, Object> uni0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "uni0");
                return params;
            }

            protected static Map<String, Object> uni1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "uni1");
                return params;
            }

            protected static Map<String, Object> uni2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "uni2");
                return params;
            }
        }

        protected static class TestCompanies {
            protected static Map<String, Object> company0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "company0");
                return params;
            }

            protected static Map<String, Object> company1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "company1");
                return params;
            }
        }

        protected static class TestCities {
            protected static Map<String, Object> city0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "city0");
                return params;
            }

            protected static Map<String, Object> city1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "city1");
                return params;
            }
        }

        protected static class TestCountries {
            protected static Map<String, Object> country0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "country0");
                return params;
            }

            protected static Map<String, Object> country1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "country1");
                return params;
            }
        }

        protected static class TestWorkAt {
            protected static Map<String, Object> f1WorkAtCompany0() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 0);
                return params;
            }

            protected static Map<String, Object> f3WorkAtCompany0() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 1);
                return params;
            }

            protected static Map<String, Object> ff21WorkAtCompany1() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 2);
                return params;
            }
        }

        protected static class TestStudyAt {
            protected static Map<String, Object> f1StudyAtUni0() {
                Map<String, Object> params = new HashMap<>();
                params.put(StudiesAt.CLASS_YEAR, 0);
                return params;
            }

            protected static Map<String, Object> ff11StudyAtUni1() {
                Map<String, Object> params = new HashMap<>();
                params.put(StudiesAt.CLASS_YEAR, 1);
                return params;
            }

            protected static Map<String, Object> ff11StudyAtUni2() {
                Map<String, Object> params = new HashMap<>();
                params.put(StudiesAt.CLASS_YEAR, 2);
                return params;
            }

            protected static Map<String, Object> f2StudyAtUni2() {
                Map<String, Object> params = new HashMap<>();
                params.put(StudiesAt.CLASS_YEAR, 3);
                return params;
            }
        }
    }

    public static class Query2GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (p1:" + Nodes.Person + " {p1}), "
                    + "(f2:" + Nodes.Person + " {f2}), "
                    + "(f3:" + Nodes.Person + " {f3}), "
                    + "(f4:" + Nodes.Person + " {f4}),\n"
                    + " (s5:" + Nodes.Person + " {s5}), "
                    + "(ff6:" + Nodes.Person + " {ff6}),"
                    + "(s7:" + Nodes.Person + " {s7}),\n"
                   /*
                   * Posts
                   */
                    + " (f3Post1:" + Nodes.Post + " {f3Post1}), (f3Post2:" + Nodes.Post + " {f3Post2}),"
                    + " (f3Post3:" + Nodes.Post + " {f3Post3}),\n"
                    + " (f4Post1:" + Nodes.Post + " {f4Post1}), (f2Post1:" + Nodes.Post + " {f2Post1}),"
                    + " (f2Post2:" + Nodes.Post + " {f2Post2}), (f2Post3:" + Nodes.Post + " {f2Post3}),\n"
                    + " (s5Post1:" + Nodes.Post + " {s5Post1}),"
                    + " (s5Post2:" + Nodes.Post + " {s5Post2}),"
                    + " (ff6Post1:" + Nodes.Post + " {ff6Post1}),\n"
                    + " (s7Post1:" + Nodes.Post + " {s7Post1}),"
                    + " (s7Post2:" + Nodes.Post + " {s7Post2}),\n"
                   /*
                   * Comments
                   */
                    + " (f2Comment1:" + Nodes.Comment + " {f2Comment1}),"
                    + " (f2Comment2:" + Nodes.Comment + " {f2Comment2}),\n"
                    + " (s5Comment1:" + Nodes.Comment + " {s5Comment1}),"
                    + " (f3Comment1:" + Nodes.Comment + " {f3Comment1}),"
                    + " (p1Comment1:" + Nodes.Comment + " {p1Comment1})\n"
                   /*
                   * RELATIONSHIP
                   */
                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                   * Person-Person
                   */
                    + "FOREACH (n IN [f3, f2, f4] | CREATE (p1)-[:" + Rels.KNOWS + "]->(n) )\n"
                    + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                    + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f3) )\n"
                    + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f2) )\n"
                    + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f4) )\n"
                    + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s5) )\n"
                    + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s7) )\n"
                    + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Comment-Person
                    */
                    + "FOREACH (n IN [f2Comment1, f2Comment2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f2) )\n"
                    + "FOREACH (n IN [p1Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(p1) )\n"
                    + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f3) )\n"
                    + "FOREACH (n IN [s5Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s5) )\n"
                   /*
                    * Comment-Post
                    */
                    + "FOREACH (n IN [f2Comment1, s5Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f3Post2) )\n"
                    + "FOREACH (n IN [f2Comment2] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(s5Comment1) )\n"
                    + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f4Post1) )\n"
                    + "FOREACH (n IN [p1Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f2Post2) )";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map("p1", TestPersons.p1(), "f2", TestPersons.f2(), "f3", TestPersons.f3(), "f4",
                    TestPersons.f4(), "s5", TestPersons.s5(), "ff6", TestPersons.ff6(), "s7",
                    TestPersons.s7(), "f3Post1", TestPosts.f3Post1(), "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(), "f4Post1", TestPosts.f4Post1(), "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(), "f2Post3", TestPosts.f2Post3(), "s5Post1",
                    TestPosts.s5Post1(), "s5Post2", TestPosts.s5Post2(), "s7Post1",
                    TestPosts.s7Post1(), "s7Post2", TestPosts.s7Post2(), "ff6Post1", TestPosts.ff6Post1(),
                    "f2Comment1", TestComments.f2Comment1(), "f2Comment2", TestComments.f2Comment2(), "s5Comment1",
                    TestComments.s5Comment1(), "f3Comment1", TestComments.f3Comment1(), "p1Comment1", TestComments.p1Comment1());
        }

        protected static class TestPersons {
            protected static Map<String, Object> p1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "person1");
                params.put(Person.LAST_NAME, "last1");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 1l);
                params.put(Person.BROWSER_USED, "1");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"person1@email1", "person1@email2"});
                params.put(Person.GENDER, "1");
                params.put(Person.LANGUAGES, new String[]{"1a", "1b"});
                params.put(Person.LOCATION_IP, "1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "f2");
                params.put(Person.LAST_NAME, "last2");
                params.put(Person.CREATION_DATE, 2l);
                params.put(Person.BIRTHDAY, 2l);
                params.put(Person.BROWSER_USED, "2");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend2@email1"});
                params.put(Person.GENDER, "2");
                params.put(Person.LANGUAGES, new String[]{"2"});
                params.put(Person.LOCATION_IP, "2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "f3");
                params.put(Person.LAST_NAME, "last3");
                params.put(Person.CREATION_DATE, 3l);
                params.put(Person.BIRTHDAY, 3l);
                params.put(Person.BROWSER_USED, "3");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend3@email1", "friend3@email2"});
                params.put(Person.GENDER, "3");
                params.put(Person.LANGUAGES, new String[]{"3a", "3b"});
                params.put(Person.LOCATION_IP, "3");
                return params;
            }

            protected static Map<String, Object> f4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "f4");
                params.put(Person.LAST_NAME, "last4");
                params.put(Person.CREATION_DATE, 4l);
                params.put(Person.BIRTHDAY, 4l);
                params.put(Person.BROWSER_USED, "4");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend4@email1"});
                params.put(Person.GENDER, "4");
                params.put(Person.LANGUAGES, new String[]{"4a", "4b"});
                params.put(Person.LOCATION_IP, "4");
                return params;
            }

            protected static Map<String, Object> s5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 5L);
                params.put(Person.FIRST_NAME, "s5");
                params.put(Person.LAST_NAME, "last5");
                params.put(Person.CREATION_DATE, 5l);
                params.put(Person.BIRTHDAY, 5l);
                params.put(Person.BROWSER_USED, "5");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"stranger5@email1"});
                params.put(Person.GENDER, "5");
                params.put(Person.LANGUAGES, new String[]{"5"});
                params.put(Person.LOCATION_IP, "5");
                return params;
            }

            protected static Map<String, Object> ff6() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 6L);
                params.put(Person.FIRST_NAME, "ff6");
                params.put(Person.LAST_NAME, "last6");
                params.put(Person.CREATION_DATE, 6l);
                params.put(Person.BIRTHDAY, 6l);
                params.put(Person.BROWSER_USED, "6");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"ff6@email1"});
                params.put(Person.GENDER, "6");
                params.put(Person.LANGUAGES, new String[]{"6a", "6b"});
                params.put(Person.LOCATION_IP, "6");
                return params;
            }

            protected static Map<String, Object> s7() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 7L);
                params.put(Person.FIRST_NAME, "s7");
                params.put(Person.LAST_NAME, "last7");
                params.put(Person.CREATION_DATE, 7l);
                params.put(Person.BIRTHDAY, 7l);
                params.put(Person.BROWSER_USED, "7");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s7@email1"});
                params.put(Person.GENDER, "7");
                params.put(Person.LANGUAGES, new String[]{"7"});
                params.put(Person.LOCATION_IP, "7");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> f3Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "[f3Post1] content");
                params.put(Post.LANGUAGE, new String[]{"3"});
                params.put(Post.IMAGE_FILE, "3");
                params.put(Message.CREATION_DATE, 4l);
                params.put(Message.BROWSER_USED, "3");
                params.put(Message.LOCATION_IP, "3");
                return params;
            }

            protected static Map<String, Object> f3Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2L);
                params.put(Message.CONTENT, "[f3Post2] content");
                params.put(Post.LANGUAGE, new String[]{"3"});
                params.put(Post.IMAGE_FILE, "3");
                params.put(Message.CREATION_DATE, 3l);
                params.put(Message.BROWSER_USED, "3");
                params.put(Message.LOCATION_IP, "3");
                return params;
            }

            protected static Map<String, Object> f3Post3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 3L);
                params.put(Message.CONTENT, "[f3Post3] content");
                params.put(Post.LANGUAGE, new String[]{"3"});
                params.put(Post.IMAGE_FILE, "3");
                params.put(Message.CREATION_DATE, 3l);
                params.put(Message.BROWSER_USED, "3");
                params.put(Message.LOCATION_IP, "3");
                return params;
            }

            protected static Map<String, Object> f4Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 4L);
                params.put(Message.CONTENT, "[f4Post1] content");
                params.put(Post.LANGUAGE, new String[]{"4"});
                params.put(Post.IMAGE_FILE, "4");
                params.put(Message.CREATION_DATE, 4l);
                params.put(Message.BROWSER_USED, "4");
                params.put(Message.LOCATION_IP, "4");
                return params;
            }

            protected static Map<String, Object> f2Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 5L);
                params.put(Message.CONTENT, "[f2Post1] content");
                params.put(Post.LANGUAGE, new String[]{"2"});
                params.put(Post.IMAGE_FILE, "2");
                params.put(Message.CREATION_DATE, 4l);
                params.put(Message.BROWSER_USED, "2");
                params.put(Message.LOCATION_IP, "2");
                return params;
            }

            protected static Map<String, Object> f2Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 6L);
                params.put(Message.CONTENT, "[f2Post2] content");
                params.put(Post.LANGUAGE, new String[]{"2"});
                params.put(Post.IMAGE_FILE, "some image file");
                params.put(Message.CREATION_DATE, 2l);
                params.put(Message.BROWSER_USED, "2");
                params.put(Message.LOCATION_IP, "2");
                return params;
            }

            protected static Map<String, Object> f2Post3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 7L);
                params.put(Message.CONTENT, "[f2Post3] content");
                params.put(Post.LANGUAGE, new String[]{"2"});
                params.put(Post.IMAGE_FILE, "2 image");
                params.put(Message.CREATION_DATE, 2l);
                params.put(Message.BROWSER_USED, "safari");
                params.put(Message.LOCATION_IP, "31.55.91.343");
                return params;
            }

            protected static Map<String, Object> s5Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 8L);
                params.put(Message.CONTENT, "[s5Post1] content");
                params.put(Post.LANGUAGE, new String[]{"5"});
                params.put(Post.IMAGE_FILE, "5");
                params.put(Message.CREATION_DATE, 1l);
                params.put(Message.BROWSER_USED, "5");
                params.put(Message.LOCATION_IP, "5");
                return params;
            }

            protected static Map<String, Object> s5Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 9L);
                params.put(Message.CONTENT, "[s5Post2] content");
                params.put(Post.LANGUAGE, new String[]{"5"});
                params.put(Post.IMAGE_FILE, "5");
                params.put(Message.CREATION_DATE, 1l);
                params.put(Message.BROWSER_USED, "5");
                params.put(Message.LOCATION_IP, "5");
                return params;
            }

            protected static Map<String, Object> s7Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 10L);
                params.put(Message.CONTENT, "[s7Post1] content");
                params.put(Post.LANGUAGE, new String[]{"7a", "7b"});
                params.put(Post.IMAGE_FILE, "7");
                params.put(Message.CREATION_DATE, 1l);
                params.put(Message.BROWSER_USED, "7");
                params.put(Message.LOCATION_IP, "7");
                return params;
            }

            protected static Map<String, Object> s7Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 11L);
                params.put(Message.CONTENT, "[s7Post2] content");
                params.put(Post.LANGUAGE, new String[]{"7"});
                params.put(Post.IMAGE_FILE, "7");
                params.put(Message.CREATION_DATE, 1l);
                params.put(Message.BROWSER_USED, "7");
                params.put(Message.LOCATION_IP, "7");
                return params;
            }

            protected static Map<String, Object> ff6Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 12L);
                params.put(Message.CONTENT, "[ff6Post1] content");
                params.put(Post.LANGUAGE, new String[]{"6"});
                params.put(Post.IMAGE_FILE, "6");
                params.put(Message.CREATION_DATE, 1l);
                params.put(Message.BROWSER_USED, "6");
                params.put(Message.LOCATION_IP, "6");
                return params;
            }
        }

        protected static class TestComments {
            protected static Map<String, Object> f2Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 13L);
                params.put(Message.CONTENT, "[f2Comment1] content");
                params.put(Message.CREATION_DATE, 2l);
                params.put(Message.BROWSER_USED, "2");
                params.put(Message.LOCATION_IP, "2");
                return params;
            }

            protected static Map<String, Object> f2Comment2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 14L);
                params.put(Message.CONTENT, "[f2Comment2] content");
                params.put(Message.CREATION_DATE, 4l);
                params.put(Message.BROWSER_USED, "2");
                params.put(Message.LOCATION_IP, "2");
                return params;
            }

            protected static Map<String, Object> s5Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 15L);
                params.put(Message.CONTENT, "[s5Comment1] content");
                params.put(Message.CREATION_DATE, 1l);
                params.put(Message.BROWSER_USED, "5");
                params.put(Message.LOCATION_IP, "5");
                return params;
            }

            protected static Map<String, Object> f3Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 16L);
                params.put(Message.CONTENT, "[f3Comment1] content");
                params.put(Message.CREATION_DATE, 3l);
                params.put(Message.BROWSER_USED, "3");
                params.put(Message.LOCATION_IP, "3");
                return params;
            }

            protected static Map<String, Object> p1Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 17L);
                params.put(Message.CONTENT, "[p1Comment1] content");
                params.put(Message.CREATION_DATE, 1l);
                params.put(Message.BROWSER_USED, "browser1");
                params.put(Message.LOCATION_IP, "1");
                return params;
            }
        }
    }

    public static class Query3GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (person1:" + Nodes.Person + " {person1}), "
                    + "(f2:" + Nodes.Person + " {f2}), "
                    + "(f3:" + Nodes.Person + " {f3}), "
                    + "(f4:" + Nodes.Person + " {f4}),\n"
                    + " (s5:" + Nodes.Person + " {s5}), "
                    + "(ff6:" + Nodes.Person + " {ff6}),"
                    + "(s7:" + Nodes.Person + " {s7}),\n"
                   /*
                   * Cities
                   */
                    + " (city1:" + Place.Type.City + " {city1}), "
                    + " (city2:" + Place.Type.City + " {city2}),"
                    + " (city3:" + Place.Type.City + " {city3}),\n"
                    + " (city4:" + Place.Type.City + " {city4}),"
                    + " (city5:" + Place.Type.City + " {city5}),\n"
                   /*
                   * Countries
                   */
                    + " (country1:" + Place.Type.Country + " {country1}),"
                    + " (country2:" + Place.Type.Country + " {country2}),\n"
                    + " (country3:" + Place.Type.Country + " {country3}),"
                    + " (country4:" + Place.Type.Country + " {country4}),\n"
                    + " (country5:" + Place.Type.Country + " {country5}),\n"
                   /*
                   * Posts
                   */
                    + " (f3Post1:" + Nodes.Post + " {f3Post1}),\n"
                    + "(f3Post2:" + Nodes.Post + " {f3Post2}),\n"
                    + " (f3Post3:" + Nodes.Post + " {f3Post3}),\n"
                    + " (f4Post1:" + Nodes.Post + " {f4Post1}),\n"
                    + "(f2Post1:" + Nodes.Post + " {f2Post1}),\n"
                    + " (f2Post2:" + Nodes.Post + " {f2Post2}),\n"
                    + "(f2Post3:" + Nodes.Post + " {f2Post3}),\n"
                    + " (s5Post1:" + Nodes.Post + " {s5Post1}),\n"
                    + " (s5Post2:" + Nodes.Post + " {s5Post2}),\n"
                    + " (ff6Post1:" + Nodes.Post + " {ff6Post1}),\n"
                    + " (s7Post1:" + Nodes.Post + " {s7Post1}),\n"
                    + " (s7Post2:" + Nodes.Post + " {s7Post2}),\n"
                   /*
                   * Comments
                   */
                    + " (f2Comment1:" + Nodes.Comment + " {f2Comment1}),"
                    + " (f2Comment2:" + Nodes.Comment + " {f2Comment2}),\n"
                    + " (s5Comment1:" + Nodes.Comment + " {s5Comment1}),"
                    + " (f3Comment1:" + Nodes.Comment + " {f3Comment1}),"
                    + " (person1Comment1:" + Nodes.Comment + " {person1Comment1}),\n"
                    + " (ff6Comment1:" + Nodes.Comment + " {ff6Comment1}),\n"
                   /*
                   * RELATIONSHIP
                   */
                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * City-Country
                    */
                    + " (city2)-[:" + Rels.IS_LOCATED_IN + "]->(country1),"
                    + " (city1)-[:" + Rels.IS_LOCATED_IN + "]->(country2),\n"
                    + " (city3)-[:" + Rels.IS_LOCATED_IN + "]->(country3),"
                    + " (city4)-[:" + Rels.IS_LOCATED_IN + "]->(country5),"
                    + " (city5)-[:" + Rels.IS_LOCATED_IN + "]->(country4),\n"
                   /*
                    * Person-City
                    */
                    + " (ff6)-[:" + Rels.IS_LOCATED_IN + "]->(city4),\n"
                    + " (person1)-[:" + Rels.IS_LOCATED_IN + "]->(city2),\n"
                    + " (f2)-[:" + Rels.IS_LOCATED_IN + "]->(city4),\n"
                    + " (f3)-[:" + Rels.IS_LOCATED_IN + "]->(city1),\n"
                    + " (f4)-[:" + Rels.IS_LOCATED_IN + "]->(city3),\n"
                    + " (s5)-[:" + Rels.IS_LOCATED_IN + "]->(city2),\n"
                    + " (s7)-[:" + Rels.IS_LOCATED_IN + "]->(city2),\n"
                   /*
                   * Comment-Country
                   */
                    + " (s5Comment1)-[:" + Rels.IS_LOCATED_IN + "]->(country1),\n"
                    + " (f2Comment2)-[:" + Rels.IS_LOCATED_IN + "]->(country4),\n"
                    + " (f3Comment1)-[:" + Rels.IS_LOCATED_IN + "]->(country3),\n"
                    + " (person1Comment1)-[:" + Rels.IS_LOCATED_IN + "]->(country2),\n"
                    + " (f2Comment1)-[:" + Rels.IS_LOCATED_IN + "]->(country1),\n"
                    + " (ff6Comment1)-[:" + Rels.IS_LOCATED_IN + "]->(country2)\n"
                   /*
                   * Post-Country
                   */
                    + "FOREACH (n IN [f3Post1,f2Post2, f2Post3] | CREATE (n)-[:" + Rels.IS_LOCATED_IN + "]->(country2) )\n"
                    + "FOREACH (n IN [ff6Post1,f3Post2,f3Post3,f2Post1,s7Post1,s7Post2,s5Post2] | CREATE (n)-[:" + Rels.IS_LOCATED_IN + "]->(country1) )\n"
                    + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.IS_LOCATED_IN + "]->(country3) )\n"
                    + "FOREACH (n IN [s5Post1] | CREATE (n)-[:" + Rels.IS_LOCATED_IN + "]->(country4) )\n"
                   /*
                   * Person-Person
                   */
                    + "FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:" + Rels.KNOWS + "]->(n) )\n"
                    + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                    + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f3) )\n"
                    + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f2) )\n"
                    + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f4) )\n"
                    + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s5) )\n"
                    + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s7) )\n"
                    + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Comment-Person
                    */
                    + "FOREACH (n IN [f2Comment1, f2Comment2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f2) )\n"
                    + "FOREACH (n IN [person1Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(person1) )\n"
                    + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f3) )\n"
                    + "FOREACH (n IN [s5Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s5) )\n"
                    + "FOREACH (n IN [ff6Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Comment-Post
                    */
                    + "FOREACH (n IN [f2Comment1, s5Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f3Post2) )\n"
                    + "FOREACH (n IN [f2Comment2] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(s5Comment1) )\n"
                    + "FOREACH (n IN [f3Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f4Post1) )\n"
                    + "FOREACH (n IN [person1Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f2Post2) )\n"
                    + "FOREACH (n IN [ff6Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f2Post3) )\n";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "city1", TestCities.city1(),
                    "city2", TestCities.city2(),
                    "city3", TestCities.city3(),
                    "city4", TestCities.city4(),
                    "city5", TestCities.city5(),
                    "country1", TestCountries.country1(),
                    "country2", TestCountries.country2(),
                    "country3", TestCountries.country3(),
                    "country4", TestCountries.country4(),
                    "country5", TestCountries.country5(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f4Post1", TestPosts.f4Post1(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "ff6Post1", TestPosts.ff6Post1(),
                    "f2Comment1", TestComments.f2Comment1(),
                    "f2Comment2", TestComments.f2Comment2(),
                    "s5Comment1", TestComments.s5Comment1(),
                    "f3Comment1", TestComments.f3Comment1(),
                    "ff6Comment1", TestComments.ff6Comment1(),
                    "person1Comment1", TestComments.person1Comment1());
        }

        protected static class TestCountries {
            protected static Map<String, Object> country1() {
                return MapUtil.map(Place.NAME, "country1");
            }

            protected static Map<String, Object> country2() {
                return MapUtil.map(Place.NAME, "country2");
            }

            protected static Map<String, Object> country3() {
                return MapUtil.map(Place.NAME, "country3");
            }

            protected static Map<String, Object> country4() {
                return MapUtil.map(Place.NAME, "country4");
            }

            protected static Map<String, Object> country5() {
                return MapUtil.map(Place.NAME, "country5");
            }
        }

        protected static class TestCities {
            protected static Map<String, Object> city1() {
                return MapUtil.map(Place.NAME, "city1");
            }

            protected static Map<String, Object> city2() {
                return MapUtil.map(Place.NAME, "city2");
            }

            protected static Map<String, Object> city3() {
                return MapUtil.map(Place.NAME, "city3");
            }

            protected static Map<String, Object> city4() {
                return MapUtil.map(Place.NAME, "city4");
            }

            protected static Map<String, Object> city5() {
                return MapUtil.map(Place.NAME, "city5");
            }
        }

        protected static class TestPersons {
            protected static Map<String, Object> person1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "person1");
                params.put(Person.LAST_NAME, "last1");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 1l);
                params.put(Person.BROWSER_USED, "browser1");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"person1a@email.com", "person1b@email.com"});
                params.put(Person.GENDER, "gender1");
                params.put(Person.LANGUAGES, new String[]{"language1a", "language1b"});
                params.put(Person.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "f2");
                params.put(Person.LAST_NAME, "last2");
                params.put(Person.CREATION_DATE, 2l);
                params.put(Person.BIRTHDAY, 2l);
                params.put(Person.BROWSER_USED, "browser2");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"});
                params.put(Person.GENDER, "gender2");
                params.put(Person.LANGUAGES, new String[]{"language2"});
                params.put(Person.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "f3");
                params.put(Person.LAST_NAME, "last3");
                params.put(Person.CREATION_DATE, 3l);
                params.put(Person.BIRTHDAY, 3l);
                params.put(Person.BROWSER_USED, "browser3");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f3a@email.com", "f3b@email.com"});
                params.put(Person.GENDER, "gender3");
                params.put(Person.LANGUAGES, new String[]{"language3a", "language3b"});
                params.put(Person.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "f4");
                params.put(Person.LAST_NAME, "last4");
                params.put(Person.CREATION_DATE, 4l);
                params.put(Person.BIRTHDAY, 4l);
                params.put(Person.BROWSER_USED, "browser4");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"});
                params.put(Person.GENDER, "gender4");
                params.put(Person.LANGUAGES, new String[]{"language4a", "language4b"});
                params.put(Person.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> s5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 5L);
                params.put(Person.FIRST_NAME, "s5");
                params.put(Person.LAST_NAME, "last5");
                params.put(Person.CREATION_DATE, 5l);
                params.put(Person.BIRTHDAY, 5l);
                params.put(Person.BROWSER_USED, "browser5");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s5@email.com"});
                params.put(Person.GENDER, "gender5");
                params.put(Person.LANGUAGES, new String[]{"language5"});
                params.put(Person.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> ff6() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 6L);
                params.put(Person.FIRST_NAME, "ff6");
                params.put(Person.LAST_NAME, "last6");
                params.put(Person.CREATION_DATE, 6l);
                params.put(Person.BIRTHDAY, 6l);
                params.put(Person.BROWSER_USED, "browser6");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"});
                params.put(Person.GENDER, "gender6");
                params.put(Person.LANGUAGES, new String[]{"language6a", "language6b"});
                params.put(Person.LOCATION_IP, "ip6");
                return params;
            }

            protected static Map<String, Object> s7() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 7L);
                params.put(Person.FIRST_NAME, "s7");
                params.put(Person.LAST_NAME, "last7");
                params.put(Person.CREATION_DATE, 7l);
                params.put(Person.BIRTHDAY, 7l);
                params.put(Person.BROWSER_USED, "browser7");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"});
                params.put(Person.GENDER, "gender7");
                params.put(Person.LANGUAGES, new String[]{"language7"});
                params.put(Person.LOCATION_IP, "ip7");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> f3Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "[f3Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2L);
                params.put(Message.CONTENT, "[f3Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 10, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 3L);
                params.put(Message.CONTENT, "[f3Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 4, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 4L);
                params.put(Message.CONTENT, "[f4Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language4"});
                params.put(Post.IMAGE_FILE, "image4");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser4");
                params.put(Message.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> f2Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 5L);
                params.put(Message.CONTENT, "[f2Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "ip2");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 10, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "ip2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 6L);
                params.put(Message.CONTENT, "[f2Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 10, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 7L);
                params.put(Message.CONTENT, "[f2Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 4, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> s5Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 8L);
                params.put(Message.CONTENT, "[s5Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s5Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 9L);
                params.put(Message.CONTENT, "[s5Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s7Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 10L);
                params.put(Message.CONTENT, "[s7Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language7a", "language7b"});
                params.put(Post.IMAGE_FILE, "image7");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> s7Post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 11L);
                params.put(Message.CONTENT, "[s7Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language7"});
                params.put(Post.IMAGE_FILE, "image7");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> ff6Post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 12L);
                params.put(Message.CONTENT, "[ff6Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language6"});
                params.put(Post.IMAGE_FILE, "image6");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 4, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser6");
                params.put(Message.LOCATION_IP, "ip6");
                return params;
            }
        }

        protected static class TestComments {
            protected static Map<String, Object> f2Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 13L);
                params.put(Message.CONTENT, "[f2Comment1] content");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Comment2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 14L);
                params.put(Message.CONTENT, "[f2Comment2] content");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 4, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> s5Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 15L);
                params.put(Message.CONTENT, "[s5Comment1] content");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> f3Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 16L);
                params.put(Message.CONTENT, "[f3Comment1] content");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> person1Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 17L);
                params.put(Message.CONTENT, "[person1Comment1] content");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser1");
                params.put(Message.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> ff6Comment1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 18L);
                params.put(Message.CONTENT, "[ff6Comment1] content");
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 4, 0, 0, 0);
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser6");
                params.put(Message.LOCATION_IP, "ip6");
                return params;
            }
        }
    }

    public static class Query4GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Tags
                    */
                    + " (tag1:" + Nodes.Tag + " {tag1}), "
                    + "(tag2:" + Nodes.Tag + " {tag2}), "
                    + "(tag3:" + Nodes.Tag + " {tag3}), "
                    + "(tag4:" + Nodes.Tag + " {tag4}), "
                    + "(tag5:" + Nodes.Tag + " {tag5}),\n"
                   /*
                    * Persons
                    */
                    + " (person1:" + Nodes.Person + " {person1}), "
                    + "(f2:" + Nodes.Person + " {f2}), "
                    + "(f3:" + Nodes.Person + " {f3}), "
                    + "(f4:" + Nodes.Person + " {f4}),\n"
                    + "(s5:" + Nodes.Person + " {s5}), "
                    + "(ff6:" + Nodes.Person + " {ff6}),"
                    + "(s7:" + Nodes.Person + " {s7}),\n"
                   /*
                   * Posts
                   */
                    + " (f3Post1:" + Nodes.Post + " {f3Post1}), (f3Post2:" + Nodes.Post + " {f3Post2}),"
                    + " (f3Post3:" + Nodes.Post + " {f3Post3}),\n"
                    + " (f4Post1:" + Nodes.Post + " {f4Post1}), (f2Post1:" + Nodes.Post + " {f2Post1}),"
                    + " (f2Post2:" + Nodes.Post + " {f2Post2}), (f2Post3:" + Nodes.Post + " {f2Post3}),\n"
                    + " (s5Post1:" + Nodes.Post + " {s5Post1}),"
                    + " (s5Post2:" + Nodes.Post + " {s5Post2}),"
                    + " (ff6Post1:" + Nodes.Post + " {ff6Post1}),\n"
                    + " (s7Post1:" + Nodes.Post + " {s7Post1}),"
                    + " (s7Post2:" + Nodes.Post + " {s7Post2}),\n"
                   /*
                   * Comments
                   */
                    + " (f4Comment1:" + Nodes.Comment + " {f4Comment1})\n"
                   /*
                   * RELATIONSHIP
                   */
                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                   * Person-Person
                   */
                    + "FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:" + Rels.KNOWS + "]->(n) )\n"
                    + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                    + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f3) )\n"
                    + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f2) )\n"
                    + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f4) )\n"
                    + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s5) )\n"
                    + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s7) )\n"
                    + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(ff6) )\n"
                   /*
                   * Comment-Person
                   */
                    + "FOREACH (n IN [f4Comment1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f4) )\n"
                   /*
                   * Comment-Post
                   */
                    + "FOREACH (n IN [f4Comment1] | CREATE (n)-[:" + Rels.REPLY_OF + "]->(f2Post1) )\n"
                   /*
                   * Post-Tag
                   */
                    + "FOREACH (n IN [f3Post1,f3Post2,f2Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag4) )\n"
                    + "FOREACH (n IN [f3Post3,ff6Post1,s7Post2] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag5) )\n"
                    + "FOREACH (n IN [f3Post3,f4Post1,f2Post2,s5Post2,ff6Post1,s7Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag3) )\n"
                    + "FOREACH (n IN [f3Post3,f4Post1,f2Post1,f2Post3,s5Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag2) )\n"
                    + "FOREACH (n IN [f3Post1,f2Post1,f2Post3,s5Post1,ff6Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag1) )"
                  /*
                   * Post-Tag
                   */
                    + "FOREACH (n IN [f4Comment1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag1) )";

        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    "tag1", TestTags.tag1(),
                    "tag2", TestTags.tag2(),
                    "tag3", TestTags.tag3(),
                    "tag4", TestTags.tag4(),
                    "tag5", TestTags.tag5(),
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f4Post1", TestPosts.f4Post1(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "f4Comment1", TestComments.f4Comment1(),
                    "ff6Post1", TestPosts.ff6Post1());
        }

        protected static class TestPersons {
            protected static Map<String, Object> person1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "person1");
                params.put(Person.LAST_NAME, "last1");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 1l);
                params.put(Person.BROWSER_USED, "browser1");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"person1b@email.com", "person1b@email.com"});
                params.put(Person.GENDER, "gender1");
                params.put(Person.LANGUAGES, new String[]{"language1a", "language1b"});
                params.put(Person.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "f2");
                params.put(Person.LAST_NAME, "last2");
                params.put(Person.CREATION_DATE, 2l);
                params.put(Person.BIRTHDAY, 2l);
                params.put(Person.BROWSER_USED, "browser2");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend2@email.com"});
                params.put(Person.GENDER, "gender2");
                params.put(Person.LANGUAGES, new String[]{"language2"});
                params.put(Person.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "f3");
                params.put(Person.LAST_NAME, "last3");
                params.put(Person.CREATION_DATE, 3l);
                params.put(Person.BIRTHDAY, 3l);
                params.put(Person.BROWSER_USED, "browser3");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend3a@email.com", "friend3b@email.com"});
                params.put(Person.GENDER, "gender3");
                params.put(Person.LANGUAGES, new String[]{"language3a", "language3b"});
                params.put(Person.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "f4");
                params.put(Person.LAST_NAME, "last4");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 1l);
                params.put(Person.BROWSER_USED, "browser4");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend4@email.com"});
                params.put(Person.GENDER, "gender4");
                params.put(Person.LANGUAGES, new String[]{"language4a", "language4b"});
                params.put(Person.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> s5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 5L);
                params.put(Person.FIRST_NAME, "s5");
                params.put(Person.LAST_NAME, "last5");
                params.put(Person.CREATION_DATE, 5l);
                params.put(Person.BIRTHDAY, 5l);
                params.put(Person.BROWSER_USED, "browser5");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"stranger5@email.com"});
                params.put(Person.GENDER, "gender5");
                params.put(Person.LANGUAGES, new String[]{"language5"});
                params.put(Person.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> ff6() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 6L);
                params.put(Person.FIRST_NAME, "ff6");
                params.put(Person.LAST_NAME, "last6");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 1l);
                params.put(Person.BROWSER_USED, "browser6");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"friend6@email.com"});
                params.put(Person.GENDER, "gender6");
                params.put(Person.LANGUAGES, new String[]{"language6a", "language6b"});
                params.put(Person.LOCATION_IP, "ip6");
                return params;
            }

            protected static Map<String, Object> s7() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 7L);
                params.put(Person.FIRST_NAME, "s7");
                params.put(Person.LAST_NAME, "last7");
                params.put(Person.CREATION_DATE, 7l);
                params.put(Person.BIRTHDAY, 7l);
                params.put(Person.BROWSER_USED, "browser7");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"stranger7a@email.com", "stranger7b@email.com"});
                params.put(Person.GENDER, "gender7");
                params.put(Person.LANGUAGES, new String[]{"language7a", "language7b"});
                params.put(Person.LOCATION_IP, "ip7");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> f3Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 2, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "[f3Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2L);
                params.put(Message.CONTENT, "[f3Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 3L);
                params.put(Message.CONTENT, "[f3Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 5, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 4L);
                params.put(Message.CONTENT, "[f4Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language4"});
                params.put(Post.IMAGE_FILE, "image4");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser4");
                params.put(Message.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> f2Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 5L);
                params.put(Message.CONTENT, "[f2Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 6L);
                params.put(Message.CONTENT, "[f2Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 7L);
                params.put(Message.CONTENT, "[f2Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> s5Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 8L);
                params.put(Message.CONTENT, "[s5Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s5Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 9L);
                params.put(Message.CONTENT, "[s5Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s7Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 10L);
                params.put(Message.CONTENT, "[s7Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language7a", "language7b"});
                params.put(Post.IMAGE_FILE, "image7");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> s7Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 11L);
                params.put(Message.CONTENT, "[s7Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language7"});
                params.put(Post.IMAGE_FILE, "image7");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> ff6Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 12L);
                params.put(Message.CONTENT, "[ff6Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language6"});
                params.put(Post.IMAGE_FILE, "image6");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser6");
                params.put(Message.LOCATION_IP, "ip6");
                return params;
            }
        }

        protected static class TestComments {
            protected static Map<String, Object> f4Comment1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 13L);
                params.put(Message.CONTENT, "[f4Comment1] content");
                params.put(Message.CREATION_DATE, c.getTime().getTime());
                params.put(Message.BROWSER_USED, "browser4");
                params.put(Message.LOCATION_IP, "ip4");
                return params;
            }
        }

        protected static class TestTags {
            protected static Map<String, Object> tag1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag1");
                params.put(Tag.URI, new String[]{"tag1 uri"});
                return params;
            }

            protected static Map<String, Object> tag2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag2");
                params.put(Tag.URI, new String[]{"tag2 uri"});
                return params;
            }

            protected static Map<String, Object> tag3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag3");
                params.put(Tag.URI, new String[]{"tag3 uri"});
                return params;
            }

            protected static Map<String, Object> tag4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag4");
                params.put(Tag.URI, new String[]{"tag4 uri"});
                return params;
            }

            protected static Map<String, Object> tag5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag5");
                params.put(Tag.URI, new String[]{"tag5 uri"});
                return params;
            }
        }
    }

    public static class Query5GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                   /*
                   * NODES
                   */
                    + "\n// --- NODES ---\n\n"
                    /*
                     * Forums
                    */
                    + " (forum1:" + Nodes.Forum + " {forum1}),"
                    + " (forum2:" + Nodes.Forum + " {forum2}),\n"
                    + " (forum3:" + Nodes.Forum + " {forum3}),"
                    + " (forum4:" + Nodes.Forum + " {forum4}),\n"
                   /*
                    * Persons
                    */
                    + " (person1:" + Nodes.Person + " {person1}), "
                    + "(f2:" + Nodes.Person + " {f2}), "
                    + "(f3:" + Nodes.Person + " {f3}), "
                    + "(f4:" + Nodes.Person + " {f4}),\n"
                    + " (s5:" + Nodes.Person + " {s5}), "
                    + "(ff6:" + Nodes.Person + " {ff6}),"
                    + "(s7:" + Nodes.Person + " {s7}),\n"
                   /*
                   * Posts
                   */
                    + " (f3Post1:" + Nodes.Post + " {f3Post1}), (f3Post2:" + Nodes.Post + " {f3Post2}),"
                    + " (f3Post3:" + Nodes.Post + " {f3Post3}),\n"
                    + " (f2Post1:" + Nodes.Post + " {f2Post1}),"
                    + " (f2Post2:" + Nodes.Post + " {f2Post2}), (f2Post3:" + Nodes.Post + " {f2Post3}),\n"
                    + " (s5Post1:" + Nodes.Post + " {s5Post1}),"
                    + " (s5Post2:" + Nodes.Post + " {s5Post2}),"
                    + " (ff6Post1:" + Nodes.Post + " {ff6Post1}),\n"
                    + " (s7Post1:" + Nodes.Post + " {s7Post1}),"
                    + " (s7Post2:" + Nodes.Post + " {s7Post2}),\n"
                   /*
                   * RELATIONSHIP
                   */
                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Forum-Person (member)
                    */
                    + " (forum1)-[:" + Rels.HAS_MEMBER + " {forum1HasMemberPerson1}]->(person1),"
                    + " (forum1)-[:" + Rels.HAS_MEMBER + " {forum1HasMemberF2}]->(f2),\n"
                    + " (forum1)-[:" + Rels.HAS_MEMBER + " {forum1HasMemberS5}]->(s5),"
                    + " (forum1)-[:" + Rels.HAS_MEMBER + " {forum1HasMemberF3}]->(f3),\n"
                    + " (forum1)-[:" + Rels.HAS_MEMBER + " {forum1HasMemberFF6}]->(ff6),\n"
                    + " (forum2)-[:" + Rels.HAS_MEMBER + " {forum2HasMemberF3}]->(f3),"
                    + " (forum3)-[:" + Rels.HAS_MEMBER + " {forum3HasMemberPerson1}]->(person1),\n"
                    + " (forum3)-[:" + Rels.HAS_MEMBER + " {forum3HasMemberF3}]->(f3),"
                    + " (forum3)-[:" + Rels.HAS_MEMBER + " {forum3HasMemberF4}]->(f4),\n"
                    + " (forum4)-[:" + Rels.HAS_MEMBER + " {forum4HasMemberF2}]->(f2),\n"
                    + " (forum4)-[:" + Rels.HAS_MEMBER + " {forum4HasMemberPerson1}]->(person1),\n"
                   /*
                   * Person-Person
                   */
                    + " (person1)-[:" + Rels.KNOWS + "]->(f2), (f2)-[:" + Rels.KNOWS + "]->(ff6),\n"
                    + " (person1)-[:" + Rels.KNOWS + "]->(f3),\n"
                    + " (person1)-[:" + Rels.KNOWS + "]->(f4)\n"
                   /*
                   * Post-Person
                   */
                    + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f3) )\n"
                    + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f2) )\n"
                    + "FOREACH (n IN [] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f4) )\n"
                    + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s5) )\n"
                    + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s7) )\n"
                    + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(ff6) )\n"
                   /*
                    * Post-Forum
                    */
                    + "FOREACH (n IN [f3Post1, f3Post2, f2Post1, f2Post2, s5Post1, s5Post2, ff6Post1]| CREATE (forum1)-[:" + Rels.CONTAINER_OF + "]->(n) )\n"
                    + "FOREACH (n IN [f3Post3] | CREATE (forum3)-[:" + Rels.CONTAINER_OF + "]->(n) )\n"
                    + "FOREACH (n IN [f2Post3] | CREATE (forum4)-[:" + Rels.CONTAINER_OF + "]->(n) )";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    "forum1", TestForums.forum1(),
                    "forum2", TestForums.forum2(),
                    "forum3", TestForums.forum3(),
                    "forum4", TestForums.forum4(),
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "ff6Post1", TestPosts.ff6Post1(),
                    "forum1HasMemberPerson1", TestHasMember.forum1HasMemberPerson1(),
                    "forum1HasMemberF2", TestHasMember.forum1HasMemberF2(),
                    "forum1HasMemberS5", TestHasMember.forum1HasMemberS5(),
                    "forum1HasMemberF3", TestHasMember.forums1HasMemberF3(),
                    "forum1HasMemberFF6", TestHasMember.forum1HasMemberFF6(),
                    "forum2HasMemberF3", TestHasMember.forum2HasMemberF3(),
                    "forum3HasMemberF3", TestHasMember.forum3HasMemberF3(),
                    "forum3HasMemberPerson1", TestHasMember.forum3HasMemberPerson1(),
                    "forum3HasMemberF4", TestHasMember.forum3HasMemberF4(),
                    "forum4HasMemberF2", TestHasMember.forum4HasMemberF2(),
                    "forum4HasMemberPerson1", TestHasMember.forum4HasMemberPerson1());
        }

        protected static class TestHasMember {
            protected static Map<String, Object> forum1HasMemberPerson1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum1HasMemberF2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum1HasMemberS5() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forums1HasMemberF3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum1HasMemberFF6() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum2HasMemberF3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum3HasMemberF3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum3HasMemberPerson1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum3HasMemberF4() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum4HasMemberF2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }

            protected static Map<String, Object> forum4HasMemberPerson1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long joinDate = c.getTimeInMillis();
                return MapUtil.map(HasMember.JOIN_DATE, joinDate);
            }
        }

        protected static class TestPersons {
            protected static Map<String, Object> person1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "person1");
                params.put(Person.LAST_NAME, "last1");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 1l);
                params.put(Person.BROWSER_USED, "browser1");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"person1a@email.com", "person1b@email.com"});
                params.put(Person.GENDER, "gender1");
                params.put(Person.LANGUAGES, new String[]{"language1a", "language1b"});
                params.put(Person.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "f2");
                params.put(Person.LAST_NAME, "last2");
                params.put(Person.CREATION_DATE, 2l);
                params.put(Person.BIRTHDAY, 2l);
                params.put(Person.BROWSER_USED, "browser2");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"});
                params.put(Person.GENDER, "gender2");
                params.put(Person.LANGUAGES, new String[]{"language2"});
                params.put(Person.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "f3");
                params.put(Person.LAST_NAME, "last3");
                params.put(Person.CREATION_DATE, 3l);
                params.put(Person.BIRTHDAY, 3l);
                params.put(Person.BROWSER_USED, "browser3");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f3a@email.com", "f3b@email.com"});
                params.put(Person.GENDER, "gender3");
                params.put(Person.LANGUAGES, new String[]{"language3a", "language3b"});
                params.put(Person.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "f4");
                params.put(Person.LAST_NAME, "last4");
                params.put(Person.CREATION_DATE, 4l);
                params.put(Person.BIRTHDAY, 4l);
                params.put(Person.BROWSER_USED, "browser4");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"});
                params.put(Person.GENDER, "gender4");
                params.put(Person.LANGUAGES, new String[]{"language4a", "language4b"});
                params.put(Person.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> s5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 5L);
                params.put(Person.FIRST_NAME, "s5");
                params.put(Person.LAST_NAME, "last5");
                params.put(Person.CREATION_DATE, 5l);
                params.put(Person.BIRTHDAY, 5l);
                params.put(Person.BROWSER_USED, "browser5");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s5@email.com"});
                params.put(Person.GENDER, "gender5");
                params.put(Person.LANGUAGES, new String[]{"language5"});
                params.put(Person.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> ff6() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 6L);
                params.put(Person.FIRST_NAME, "ff6");
                params.put(Person.LAST_NAME, "last6");
                params.put(Person.CREATION_DATE, 6l);
                params.put(Person.BIRTHDAY, 6l);
                params.put(Person.BROWSER_USED, "browser6");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"});
                params.put(Person.GENDER, "gender6");
                params.put(Person.LANGUAGES, new String[]{"language6a", "language6b"});
                params.put(Person.LOCATION_IP, "ip6");
                return params;
            }

            protected static Map<String, Object> s7() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 7L);
                params.put(Person.FIRST_NAME, "s7");
                params.put(Person.LAST_NAME, "last7");
                params.put(Person.CREATION_DATE, 7l);
                params.put(Person.BIRTHDAY, 7l);
                params.put(Person.BROWSER_USED, "browser7");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"});
                params.put(Person.GENDER, "gender7");
                params.put(Person.LANGUAGES, new String[]{"language7a", "language7b"});
                params.put(Person.LOCATION_IP, "ip7");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> f3Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "[f3Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2L);
                params.put(Message.CONTENT, "[f3Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 3L);
                params.put(Message.CONTENT, "[f3Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f2Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 5L);
                params.put(Message.CONTENT, "[f2Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 6L);
                params.put(Message.CONTENT, "[f2Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 7L);
                params.put(Message.CONTENT, "[f2Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> s5Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 8L);
                params.put(Message.CONTENT, "[s5Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s5Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 9L);
                params.put(Message.CONTENT, "[s5Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s7Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 10L);
                params.put(Message.CONTENT, "[s7Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language7a", "language7b"});
                params.put(Post.IMAGE_FILE, "image7");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> s7Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 11L);
                params.put(Message.CONTENT, "[s7Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language7"});
                params.put(Post.IMAGE_FILE, "image7");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> ff6Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 12L);
                params.put(Message.CONTENT, "[ff6Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language6"});
                params.put(Post.IMAGE_FILE, "image6");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser6");
                params.put(Message.LOCATION_IP, "ip6");
                return params;
            }
        }

        protected static class TestForums {
            protected static Map<String, Object> forum1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Forum.TITLE, "forum1");
                params.put(Forum.CREATION_DATE, creationDate);
                return params;
            }

            protected static Map<String, Object> forum2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Forum.TITLE, "forum2");
                params.put(Forum.CREATION_DATE, creationDate);
                return params;
            }

            protected static Map<String, Object> forum3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Forum.TITLE, "forum3");
                params.put(Forum.CREATION_DATE, creationDate);
                return params;
            }

            protected static Map<String, Object> forum4() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Forum.TITLE, "forum4");
                params.put(Forum.CREATION_DATE, creationDate);
                return params;
            }
        }
    }

    public static class Query6GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"

                    + "\n// --- NODES ---\n\n"

                   /*
                    * Tags
                    */
                    + " (tag1:" + Nodes.Tag + " {tag1}), "
                    + "(tag2:" + Nodes.Tag + " {tag2}), "
                    + "(tag3:" + Nodes.Tag + " {tag3}), "
                    + "(tag4:" + Nodes.Tag + " {tag4}), "
                    + "(tag5:" + Nodes.Tag + " {tag5}),\n"
                   /*
                    * Persons
                    */
                    + " (person1:" + Nodes.Person + " {person1}), "
                    + "(f2:" + Nodes.Person + " {f2}), "
                    + "(f3:" + Nodes.Person + " {f3}), "
                    + "(f4:" + Nodes.Person + " {f4}),\n"
                    + " (s5:" + Nodes.Person + " {s5}), "
                    + "(ff6:" + Nodes.Person + " {ff6}),"
                    + "(s7:" + Nodes.Person + " {s7}),\n"
                   /*
                   * Posts
                   */
                    + " (f3Post1:" + Nodes.Post + " {f3Post1}), (f3Post2:" + Nodes.Post + " {f3Post2}),"
                    + " (f3Post3:" + Nodes.Post + " {f3Post3}),\n"
                    + " (f4Post1:" + Nodes.Post + " {f4Post1}), (f2Post1:" + Nodes.Post + " {f2Post1}),"
                    + " (f2Post2:" + Nodes.Post + " {f2Post2}), (f2Post3:" + Nodes.Post + " {f2Post3}),\n"
                    + " (s5Post1:" + Nodes.Post + " {s5Post1}),"
                    + " (s5Post2:" + Nodes.Post + " {s5Post2}),"
                    + " (ff6Post1:" + Nodes.Post + " {ff6Post1}),\n"
                    + " (s7Post1:" + Nodes.Post + " {s7Post1}),"
                    + " (s7Post2:" + Nodes.Post + " {s7Post2})\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"

                   /*
                   * Person-Person
                   */
                    + "FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:" + Rels.KNOWS + "]->(n) )\n"
                    + "FOREACH (n IN [ff6] | CREATE (f2)-[:" + Rels.KNOWS + "]->(n) )\n"
                   /*
                   * Post-Person
                   */
                    + "FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f3) )\n"
                    + "FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f2) )\n"
                    + "FOREACH (n IN [f4Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(f4) )\n"
                    + "FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s5) )\n"
                    + "FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(s7) )\n"
                    + "FOREACH (n IN [ff6Post1] | CREATE (n)-[:" + Rels.HAS_CREATOR + "]->(ff6) )\n"
                   /*
                   * Post-Tag
                   */
                    + "FOREACH (n IN [f3Post1,f3Post2,f2Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag4) )\n"
                    + "FOREACH (n IN [f3Post3,ff6Post1,s7Post2] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag5) )\n"
                    + "FOREACH (n IN [f3Post3,f4Post1,f2Post2,s5Post2,ff6Post1,s7Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag3) )\n"
                    + "FOREACH (n IN [f3Post3,f4Post1,f2Post1,f2Post3,s5Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag2) )\n"
                    + "FOREACH (n IN [f3Post1,f2Post1,f2Post3,s5Post1,ff6Post1] | CREATE (n)-[:" + Rels.HAS_TAG + "]->(tag1) )\n";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    "tag1", TestTags.tag1(),
                    "tag2", TestTags.tag2(),
                    "tag3", TestTags.tag3(),
                    "tag4", TestTags.tag4(),
                    "tag5", TestTags.tag5(),
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "s5", TestPersons.s5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "f3Post1", TestPosts.f3Post1(),
                    "f3Post2", TestPosts.f3Post2(),
                    "f3Post3", TestPosts.f3Post3(),
                    "f4Post1", TestPosts.f4Post1(),
                    "f2Post1", TestPosts.f2Post1(),
                    "f2Post2", TestPosts.f2Post2(),
                    "f2Post3", TestPosts.f2Post3(),
                    "s5Post1", TestPosts.s5Post1(),
                    "s5Post2", TestPosts.s5Post2(),
                    "s7Post1", TestPosts.s7Post1(),
                    "s7Post2", TestPosts.s7Post2(),
                    "ff6Post1", TestPosts.ff6Post1());
        }

        protected static class TestPersons {
            protected static Map<String, Object> person1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "person1");
                params.put(Person.LAST_NAME, "last1");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 1l);
                params.put(Person.BROWSER_USED, "browser1");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"person1@email.com"});
                params.put(Person.GENDER, "gender1");
                params.put(Person.LANGUAGES, new String[]{"language1"});
                params.put(Person.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "f2");
                params.put(Person.LAST_NAME, "last2");
                params.put(Person.CREATION_DATE, 2l);
                params.put(Person.BIRTHDAY, 2l);
                params.put(Person.BROWSER_USED, "browser2");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"});
                params.put(Person.GENDER, "gender2");
                params.put(Person.LANGUAGES, new String[]{"language2"});
                params.put(Person.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "f3");
                params.put(Person.LAST_NAME, "last3");
                params.put(Person.CREATION_DATE, 3l);
                params.put(Person.BIRTHDAY, 3l);
                params.put(Person.BROWSER_USED, "browser3");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f3@email.com"});
                params.put(Person.GENDER, "gender3");
                params.put(Person.LANGUAGES, new String[]{"language3"});
                params.put(Person.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "f4");
                params.put(Person.LAST_NAME, "last4");
                params.put(Person.CREATION_DATE, 4l);
                params.put(Person.BIRTHDAY, 4l);
                params.put(Person.BROWSER_USED, "browser4");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"});
                params.put(Person.GENDER, "gender4");
                params.put(Person.LANGUAGES, new String[]{"language4"});
                params.put(Person.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> s5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 5L);
                params.put(Person.FIRST_NAME, "s5");
                params.put(Person.LAST_NAME, "last5");
                params.put(Person.CREATION_DATE, 5l);
                params.put(Person.BIRTHDAY, 5l);
                params.put(Person.BROWSER_USED, "browser5");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f5@email.com"});
                params.put(Person.GENDER, "gender5");
                params.put(Person.LANGUAGES, new String[]{"language5"});
                params.put(Person.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> ff6() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 6L);
                params.put(Person.FIRST_NAME, "ff6");
                params.put(Person.LAST_NAME, "last6");
                params.put(Person.CREATION_DATE, 6l);
                params.put(Person.BIRTHDAY, 6l);
                params.put(Person.BROWSER_USED, "browser6");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"});
                params.put(Person.GENDER, "gender6");
                params.put(Person.LANGUAGES, new String[]{"language6"});
                params.put(Person.LOCATION_IP, "ip6");
                return params;
            }

            protected static Map<String, Object> s7() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 7L);
                params.put(Person.FIRST_NAME, "s7");
                params.put(Person.LAST_NAME, "last7");
                params.put(Person.CREATION_DATE, 7l);
                params.put(Person.BIRTHDAY, 7l);
                params.put(Person.BROWSER_USED, "browser7");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"});
                params.put(Person.GENDER, "gender7");
                params.put(Person.LANGUAGES, new String[]{"language7"});
                params.put(Person.LOCATION_IP, "ip7");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> f3Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "[f3Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2L);
                params.put(Message.CONTENT, "[f3Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f3Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 3L);
                params.put(Message.CONTENT, "[f3Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "image3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 4L);
                params.put(Message.CONTENT, "[f4Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language4"});
                params.put(Post.IMAGE_FILE, "image4");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser4");
                params.put(Message.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> f2Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 5L);
                params.put(Message.CONTENT, "[f2Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 6L);
                params.put(Message.CONTENT, "[f2Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f2Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 7L);
                params.put(Message.CONTENT, "[f2Post3] content");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "image2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> s5Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 8L);
                params.put(Message.CONTENT, "[s5Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s5Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 9L);
                params.put(Message.CONTENT, "[s5Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language5"});
                params.put(Post.IMAGE_FILE, "image5");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> s7Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 10L);
                params.put(Message.CONTENT, "[s7Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language7"});
                params.put(Post.IMAGE_FILE, "image7");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> s7Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 11L);
                params.put(Message.CONTENT, "[s7Post2] content");
                params.put(Post.LANGUAGE, new String[]{"language7"});
                params.put(Post.IMAGE_FILE, "image7");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser7");
                params.put(Message.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> ff6Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 3, 0, 0, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 12L);
                params.put(Message.CONTENT, "[ff6Post1] content");
                params.put(Post.LANGUAGE, new String[]{"language6"});
                params.put(Post.IMAGE_FILE, "image6");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser6");
                params.put(Message.LOCATION_IP, "ip6");
                return params;
            }
        }

        protected static class TestTags {
            protected static Map<String, Object> tag1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag1");
                params.put(Tag.URI, new String[]{"tag1 uri"});
                return params;
            }

            protected static Map<String, Object> tag2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag2");
                params.put(Tag.URI, new String[]{"tag2 uri"});
                return params;
            }

            protected static Map<String, Object> tag3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag3");
                params.put(Tag.URI, new String[]{"tag3 uri"});
                return params;
            }

            protected static Map<String, Object> tag4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag4");
                params.put(Tag.URI, new String[]{"tag4 uri"});
                return params;
            }

            protected static Map<String, Object> tag5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag5");
                params.put(Tag.URI, new String[]{"tag5 uri"});
                return params;
            }
        }
    }

    public static class Query7GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + "(person1:" + Nodes.Person + " {person1}),\n"
                    + "(f2:" + Nodes.Person + " {f2}),\n"
                    + "(f3:" + Nodes.Person + " {f3}),\n"
                    + "(f4:" + Nodes.Person + " {f4}),\n"
                    + "(ff5:" + Nodes.Person + " {ff5}),\n"
                    + "(ff6:" + Nodes.Person + " {ff6}),\n"
                    + "(s7:" + Nodes.Person + " {s7}),\n"
                    + "(s8:" + Nodes.Person + " {s8}),\n"
                   /*
                    * Posts
                    */
                    + "(person1Post1:" + Nodes.Post + " {person1Post1}),\n"
                    + "(person1Post2:" + Nodes.Post + " {person1Post2}),\n"
                    + "(person1Post3:" + Nodes.Post + " {person1Post3}),\n"
                    + "(s7Post1:" + Nodes.Post + " {s7Post1}),\n"

                   /*
                    * Comments
                    */
                    + "(person1Comment1:" + Nodes.Comment + " {person1Comment1}),\n"
                    + "(f4Comment1:" + Nodes.Comment + " {f4Comment1}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                    + "(person1)-[:" + Rels.KNOWS + "]->(f2),\n"
                    + "(person1)-[:" + Rels.KNOWS + "]->(f3),\n"
                    + "(person1)-[:" + Rels.KNOWS + "]->(f4),\n"
                    + "(f2)-[:" + Rels.KNOWS + "]->(ff5),\n"
                    + "(f3)-[:" + Rels.KNOWS + "]->(ff6),\n"
                   /*
                    * Comment-Post
                    */
                    + "(person1Comment1)-[:" + Rels.REPLY_OF + "]->(s7Post1),\n"
                    + "(f4Comment1)-[:" + Rels.REPLY_OF + "]->(person1Post3),\n"
                   /*
                    * Person-Like->Post
                    */
                    + "(person1)-[:" + Rels.LIKES + " {person1LikesPerson1Post1}]->(person1Post1),\n"
                    + "(person1)-[:" + Rels.LIKES + " {person1LikesS7Post1}]->(s7Post1),\n"
                    + "(f2)-[:" + Rels.LIKES + " {f2LikesPerson1Post1}]->(person1Post1),\n"
                    + "(f4)-[:" + Rels.LIKES + " {f4LikesPerson1Post1}]->(person1Post1),\n"
                    + "(f4)-[:" + Rels.LIKES + " {f4LikesPerson1Post2}]->(person1Post2),\n"
                    + "(f4)-[:" + Rels.LIKES + " {f4LikesPerson1Post3}]->(person1Post3),\n"
                    + "(ff6)-[:" + Rels.LIKES + " {ff6OldLikesPerson1Post1}]->(person1Post1),\n"
                    + "(ff6)-[:" + Rels.LIKES + " {ff6NewLikesPerson1Post1}]->(person1Post1),\n"
                    + "(ff6)-[:" + Rels.LIKES + " {ff6LikesPerson1Post2}]->(person1Post2),\n"
                    + "(s7)-[:" + Rels.LIKES + " {s7LikesPerson1Post1}]->(person1Post1),\n"
                    + "(s8)-[:" + Rels.LIKES + " {s8LikesPerson1Post2}]->(person1Post2),\n"
                   /*
                    * Person-Like->Comment
                    */
                    + "(person1)-[:" + Rels.LIKES + " {person1LikesF4Comment1}]->(f4Comment1),\n"
                    + "(s7)-[:" + Rels.LIKES + " {s7LikesPerson1Comment1}]->(person1Comment1),\n"
                    + "(s8)-[:" + Rels.LIKES + " {s8LikesF4Comment1}]->(f4Comment1),\n"
                   /*
                    * Post-Create->Person
                    */
                    + "(person1)<-[:" + Rels.HAS_CREATOR + "]-(person1Post1),\n"
                    + "(person1)<-[:" + Rels.HAS_CREATOR + "]-(person1Post2),\n"
                    + "(person1)<-[:" + Rels.HAS_CREATOR + "]-(person1Post3),\n"
                    + "(s7)<-[:" + Rels.HAS_CREATOR + "]-(s7Post1),\n"
                   /*
                    * Comment-Person
                    */
                    + "(person1)<-[:" + Rels.HAS_CREATOR + "]-(person1Comment1),\n"
                    + "(f4)<-[:" + Rels.HAS_CREATOR + "]-(f4Comment1)\n";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    // Persons
                    "person1", TestPersons.person1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "ff5", TestPersons.ff5(),
                    "ff6", TestPersons.ff6(),
                    "s7", TestPersons.s7(),
                    "s8", TestPersons.s8(),
                    // Person-Post (like)
                    "person1LikesPerson1Post1", TestPostLikes.person1LikesPerson1Post1(),
                    "person1LikesS7Post1", TestPostLikes.person1LikesS7Post1(),
                    "f2LikesPerson1Post1", TestPostLikes.f2LikesPerson1Post1(),
                    "f4LikesPerson1Post1", TestPostLikes.f4LikesPerson1Post1(),
                    "f4LikesPerson1Post2", TestPostLikes.f4LikesPerson1Post2(),
                    "f4LikesPerson1Post3", TestPostLikes.f4LikesPerson1Post3(),
                    "ff6OldLikesPerson1Post1", TestPostLikes.ff6OldLikesPerson1Post1(),
                    "ff6NewLikesPerson1Post1", TestPostLikes.ff6NewLikesPerson1Post1(),
                    "ff6LikesPerson1Post2", TestPostLikes.ff6LikesPerson1Post2(),
                    "s7LikesPerson1Post1", TestPostLikes.s7LikesPerson1Post1(),
                    "s8LikesPerson1Post2", TestPostLikes.s8LikesPerson1Post2(),
                    //Person-Comment (like)
                    "person1LikesF4Comment1", TestCommentLikes.person1LikesF4Comment1(),
                    "s7LikesPerson1Comment1", TestCommentLikes.s7LikesPerson1Comment1(),
                    "s8LikesF4Comment1", TestCommentLikes.s8LikesF4Comment1(),
                    // Posts
                    "person1Post1", TestPosts.person1Post1(),
                    "person1Post2", TestPosts.person1Post2(),
                    "person1Post3", TestPosts.person1Post3(),
                    "s7Post1", TestPosts.s7Post1(),
                    // Comments
                    "person1Comment1", TestComments.person1Comment1(),
                    "f4Comment1", TestComments.f4Comment1()
            );
        }

        protected static class TestPersons {
            protected static Map<String, Object> person1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "person1");
                params.put(Person.LAST_NAME, "last1");
                params.put(Person.CREATION_DATE, 1l);
                params.put(Person.BIRTHDAY, 2l);
                params.put(Person.BROWSER_USED, "browser1");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"person1@email.com"});
                params.put(Person.GENDER, "gender1");
                params.put(Person.LANGUAGES, new String[]{"language1"});
                params.put(Person.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "f2");
                params.put(Person.LAST_NAME, "last2");
                params.put(Person.CREATION_DATE, 2l);
                params.put(Person.BIRTHDAY, 2l);
                params.put(Person.BROWSER_USED, "browser2");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f2@email.com"});
                params.put(Person.GENDER, "gender2");
                params.put(Person.LANGUAGES, new String[]{"language2"});
                params.put(Person.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "f3");
                params.put(Person.LAST_NAME, "last3");
                params.put(Person.CREATION_DATE, 3l);
                params.put(Person.BIRTHDAY, 3l);
                params.put(Person.BROWSER_USED, "browser3");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f3@email.com"});
                params.put(Person.GENDER, "gender3");
                params.put(Person.LANGUAGES, new String[]{"language3"});
                params.put(Person.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> f4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "f4");
                params.put(Person.LAST_NAME, "last4");
                params.put(Person.CREATION_DATE, 4l);
                params.put(Person.BIRTHDAY, 4l);
                params.put(Person.BROWSER_USED, "browser4");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"f4@email.com"});
                params.put(Person.GENDER, "gender4");
                params.put(Person.LANGUAGES, new String[]{"language4"});
                params.put(Person.LOCATION_IP, "ip4");
                return params;
            }

            protected static Map<String, Object> ff5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 5L);
                params.put(Person.FIRST_NAME, "ff5");
                params.put(Person.LAST_NAME, "last5");
                params.put(Person.CREATION_DATE, 5l);
                params.put(Person.BIRTHDAY, 5l);
                params.put(Person.BROWSER_USED, "browser5");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"ff5@email.com"});
                params.put(Person.GENDER, "gender5");
                params.put(Person.LANGUAGES, new String[]{"language5"});
                params.put(Person.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> ff6() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 6L);
                params.put(Person.FIRST_NAME, "ff6");
                params.put(Person.LAST_NAME, "last6");
                params.put(Person.CREATION_DATE, 6l);
                params.put(Person.BIRTHDAY, 6l);
                params.put(Person.BROWSER_USED, "browser6");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"ff6@email.com"});
                params.put(Person.GENDER, "gender6");
                params.put(Person.LANGUAGES, new String[]{"language6"});
                params.put(Person.LOCATION_IP, "ip6");
                return params;
            }

            protected static Map<String, Object> s7() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 7L);
                params.put(Person.FIRST_NAME, "s7");
                params.put(Person.LAST_NAME, "last7");
                params.put(Person.CREATION_DATE, 7l);
                params.put(Person.BIRTHDAY, 7l);
                params.put(Person.BROWSER_USED, "browser7");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s7@email.com"});
                params.put(Person.GENDER, "gender7");
                params.put(Person.LANGUAGES, new String[]{"language7"});
                params.put(Person.LOCATION_IP, "ip7");
                return params;
            }

            protected static Map<String, Object> s8() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 8L);
                params.put(Person.FIRST_NAME, "s8");
                params.put(Person.LAST_NAME, "last8");
                params.put(Person.CREATION_DATE, 8l);
                params.put(Person.BIRTHDAY, 8l);
                params.put(Person.BROWSER_USED, "browser8");
                params.put(Person.EMAIL_ADDRESSES, new String[]{"s8@email.com"});
                params.put(Person.GENDER, "gender8");
                params.put(Person.LANGUAGES, new String[]{"language8"});
                params.put(Person.LOCATION_IP, "ip8");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> person1Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 1, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "person1post1");
                params.put(Post.LANGUAGE, new String[]{"language1"});
                params.put(Post.IMAGE_FILE, "imageFile1");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser1");
                params.put(Message.LOCATION_IP, "ip1");
                return params;
            }

            protected static Map<String, Object> person1Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 2, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2L);
                params.put(Message.CONTENT, "person1post2");
                params.put(Post.LANGUAGE, new String[]{"language2"});
                params.put(Post.IMAGE_FILE, "imageFile2");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser2");
                params.put(Message.LOCATION_IP, "ip2");
                return params;
            }

            protected static Map<String, Object> person1Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 3, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 3L);
                params.put(Message.CONTENT, "person1post3");
                params.put(Post.LANGUAGE, new String[]{"language3"});
                params.put(Post.IMAGE_FILE, "imageFile3");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }

            protected static Map<String, Object> s7Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 4, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 4L);
                params.put(Message.CONTENT, "s7post1");
                params.put(Post.LANGUAGE, new String[]{"language4"});
                params.put(Post.IMAGE_FILE, "imageFile4");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser3");
                params.put(Message.LOCATION_IP, "ip3");
                return params;
            }
        }

        protected static class TestComments {
            protected static Map<String, Object> person1Comment1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 5, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 5L);
                params.put(Message.CONTENT, "person1comment1");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser5");
                params.put(Message.LOCATION_IP, "ip5");
                return params;
            }

            protected static Map<String, Object> f4Comment1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 6, 0);
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 6L);
                params.put(Message.CONTENT, "f4comment1");
                params.put(Message.CREATION_DATE, creationDate);
                params.put(Message.BROWSER_USED, "browser6");
                params.put(Message.LOCATION_IP, "ip6");
                return params;
            }
        }

        protected static class TestPostLikes {
            protected static Map<String, Object> person1LikesPerson1Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 1, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> person1LikesS7Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 20, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> f2LikesPerson1Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 5, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> f4LikesPerson1Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 3, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> f4LikesPerson1Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 4, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> f4LikesPerson1Post3() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 5, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> ff6OldLikesPerson1Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 2, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> ff6NewLikesPerson1Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 4, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> ff6LikesPerson1Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 3, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> s7LikesPerson1Post1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 2, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> s8LikesPerson1Post2() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 10, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }
        }

        protected static class TestCommentLikes {
            protected static Map<String, Object> person1LikesF4Comment1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 20, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> s7LikesPerson1Comment1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 6, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }

            protected static Map<String, Object> s8LikesF4Comment1() {
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2000, Calendar.JANUARY, 1, 0, 20, 0);
                long creationDate = c.getTimeInMillis();

                return MapUtil.map(Likes.CREATION_DATE, creationDate);
            }
        }
    }

    public static class Query8GraphMaker implements QueryGraphMaker {

        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (person:" + Nodes.Person + " {person}),\n"
                    + " (friend1:" + Nodes.Person + " {friend1}),\n"
                    + " (friend2:" + Nodes.Person + " {friend2}),\n"
                    + " (friend3:" + Nodes.Person + " {friend3}),\n"
                   /*
                    * Posts
                    */
                    + " (post0:" + Nodes.Post + " {post0}),\n"
                    + " (post1:" + Nodes.Post + " {post1}),\n"
                    + " (post2:" + Nodes.Post + " {post2}),\n"
                   /*
                    * Comments
                    */
                    + " (comment11:" + Nodes.Comment + " {comment11}),\n"
                    + " (comment12:" + Nodes.Comment + " {comment12}),\n"
                    + " (comment13:" + Nodes.Comment + " {comment13}),\n"
                    + " (comment131:" + Nodes.Comment + " {comment131}),\n"
                    + " (comment111:" + Nodes.Comment + " {comment111}),\n"
                    + " (comment112:" + Nodes.Comment + " {comment112}),\n"
                    + " (comment21:" + Nodes.Comment + " {comment21}),\n"
                    + " (comment211:" + Nodes.Comment + " {comment211}),\n"
                    + " (comment2111:" + Nodes.Comment + " {comment2111}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Post
                    */
                    + "(person)<-[:" + Rels.HAS_CREATOR + "]-(post0),\n"
                    + "(person)<-[:" + Rels.HAS_CREATOR + "]-(post1),\n"
                    + "(person)<-[:" + Rels.HAS_CREATOR + "]-(post2),\n"
                   /*
                    * Person-Comment
                    */
                    + "(person)<-[:" + Rels.HAS_CREATOR + "]-(comment13),\n"
                    + "(friend1)<-[:" + Rels.HAS_CREATOR + "]-(comment111),\n"
                    + "(friend1)<-[:" + Rels.HAS_CREATOR + "]-(comment21),\n"
                    + "(friend1)<-[:" + Rels.HAS_CREATOR + "]-(comment2111),\n"
                    + "(friend2)<-[:" + Rels.HAS_CREATOR + "]-(comment211),\n"
                    + "(friend2)<-[:" + Rels.HAS_CREATOR + "]-(comment131),\n"
                    + "(friend2)<-[:" + Rels.HAS_CREATOR + "]-(comment112),\n"
                    + "(friend3)<-[:" + Rels.HAS_CREATOR + "]-(comment11),\n"
                    + "(friend3)<-[:" + Rels.HAS_CREATOR + "]-(comment12),\n"
                   /*
                    * Comment-Post/Comment
                    */
                    + "(post1)<-[:" + Rels.REPLY_OF + "]-(comment11)<-[:" + Rels.REPLY_OF + "]-(comment111), (comment11)<-[:" + Rels.REPLY_OF + "]-(comment112),\n"
                    + "(post1)<-[:" + Rels.REPLY_OF + "]-(comment12),\n"
                    + "(post1)<-[:" + Rels.REPLY_OF + "]-(comment13)<-[:" + Rels.REPLY_OF + "]-(comment131),\n"
                    + "(post2)<-[:" + Rels.REPLY_OF + "]-(comment21)<-[:" + Rels.REPLY_OF + "]-(comment211)<-[:" + Rels.REPLY_OF + "]-(comment2111)";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    // Persons
                    "person", TestPersons.person(),
                    "friend1", TestPersons.friend1(),
                    "friend2", TestPersons.friend2(),
                    "friend3", TestPersons.friend3(),
                    // Posts
                    "post0", TestPosts.post0(),
                    "post1", TestPosts.post1(),
                    "post2", TestPosts.post2(),
                    "post3", TestPosts.post3(),
                    // Comments
                    "comment11", TestComments.comment11(),
                    "comment12", TestComments.comment12(),
                    "comment13", TestComments.comment13(),
                    "comment131", TestComments.comment131(),
                    "comment111", TestComments.comment111(),
                    "comment112", TestComments.comment112(),
                    "comment21", TestComments.comment21(),
                    "comment211", TestComments.comment211(),
                    "comment2111", TestComments.comment2111()
            );
        }

        protected static class TestPersons {
            protected static Map<String, Object> person() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 0L);
                params.put(Person.FIRST_NAME, "person");
                params.put(Person.LAST_NAME, "zero");
                return params;
            }

            protected static Map<String, Object> friend1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "one");
                return params;
            }

            protected static Map<String, Object> friend2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "two");
                return params;
            }

            protected static Map<String, Object> friend3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "three");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> post0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 0L);
                params.put(Message.CONTENT, "P0");
                return params;
            }

            protected static Map<String, Object> post1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "P1");
                return params;
            }

            protected static Map<String, Object> post2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2L);
                params.put(Message.CONTENT, "P2");
                return params;
            }

            protected static Map<String, Object> post3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 3L);
                params.put(Message.CONTENT, "P3");
                return params;
            }
        }

        protected static class TestComments {
            protected static Map<String, Object> comment11() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 11L);
                params.put(Message.CREATION_DATE, 3L);
                params.put(Message.CONTENT, "C11");
                return params;
            }

            protected static Map<String, Object> comment12() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 12L);
                params.put(Message.CREATION_DATE, 6L);
                params.put(Message.CONTENT, "C12");
                return params;
            }

            protected static Map<String, Object> comment13() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 13L);
                params.put(Message.CREATION_DATE, 8L);
                params.put(Message.CONTENT, "C13");
                return params;
            }

            protected static Map<String, Object> comment131() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 131L);
                params.put(Message.CREATION_DATE, 9L);
                params.put(Message.CONTENT, "C131");
                return params;
            }

            protected static Map<String, Object> comment111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 111L);
                params.put(Message.CREATION_DATE, 4L);
                params.put(Message.CONTENT, "C111");
                return params;
            }

            protected static Map<String, Object> comment112() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 112L);
                params.put(Message.CREATION_DATE, 4L);
                params.put(Message.CONTENT, "C112");
                return params;
            }

            protected static Map<String, Object> comment21() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 21L);
                params.put(Message.CREATION_DATE, 1L);
                params.put(Message.CONTENT, "C21");
                return params;
            }

            protected static Map<String, Object> comment211() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 211L);
                params.put(Message.CREATION_DATE, 2L);
                params.put(Message.CONTENT, "C211");
                return params;
            }

            protected static Map<String, Object> comment2111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2111L);
                params.put(Message.CREATION_DATE, 5L);
                params.put(Message.CONTENT, "C2111");
                return params;
            }
        }
    }

    public static class Query9GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (person0:" + Nodes.Person + " {person0}),\n"
                    + " (friend1:" + Nodes.Person + " {friend1}),\n"
                    + " (friend2:" + Nodes.Person + " {friend2}),\n"
                    + " (stranger3:" + Nodes.Person + " {stranger3}),\n"
                    + " (friendfriend4:" + Nodes.Person + " {friendfriend4}),\n"
                   /*
                    * Posts
                    */
                    + " (post01:" + Nodes.Post + " {post01}),\n"
                    + " (post11:" + Nodes.Post + " {post11}),\n"
                    + " (post12:" + Nodes.Post + " {post12}),\n"
                    + " (post21:" + Nodes.Post + " {post21}),\n"
                    + " (post31:" + Nodes.Post + " {post31}),\n"
                   /*
                    * Comments
                    */
                    + " (comment111:" + Nodes.Comment + " {comment111}),\n"
                    + " (comment121:" + Nodes.Comment + " {comment121}),\n"
                    + " (comment1211:" + Nodes.Comment + " {comment1211}),\n"
                    + " (comment211:" + Nodes.Comment + " {comment211}),\n"
                    + " (comment2111:" + Nodes.Comment + " {comment2111}),\n"
                    + " (comment21111:" + Nodes.Comment + " {comment21111}),\n"
                    + " (comment311:" + Nodes.Comment + " {comment311}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                    + "(person0)-[:" + Rels.KNOWS + "]->(friend1)-[:" + Rels.KNOWS + "]->(friendfriend4),\n"
                    + "(person0)-[:" + Rels.KNOWS + "]->(friend2),\n"
                   /*
                    * Person-Post
                    */
                    + "(person0)<-[:" + Rels.HAS_CREATOR + "]-(post01),\n"
                    + "(friend1)<-[:" + Rels.HAS_CREATOR + "]-(post11),\n"
                    + "(friend1)<-[:" + Rels.HAS_CREATOR + "]-(post12),\n"
                    + "(friend2)<-[:" + Rels.HAS_CREATOR + "]-(post21),\n"
                    + "(stranger3)<-[:" + Rels.HAS_CREATOR + "]-(post31),\n"
                   /*
                    * Person-Comment
                    */
                    + "(person0)<-[:" + Rels.HAS_CREATOR + "]-(comment111),\n"
                    + "(person0)<-[:" + Rels.HAS_CREATOR + "]-(comment121),\n"
                    + "(friend1)<-[:" + Rels.HAS_CREATOR + "]-(comment211),\n"
                    + "(friend1)<-[:" + Rels.HAS_CREATOR + "]-(comment21111),\n"
                    + "(friend2)<-[:" + Rels.HAS_CREATOR + "]-(comment2111),\n"
                    + "(friend2)<-[:" + Rels.HAS_CREATOR + "]-(comment311),\n"
                    + "(stranger3)<-[:" + Rels.HAS_CREATOR + "]-(comment2111),\n"
                    + "(friendfriend4)<-[:" + Rels.HAS_CREATOR + "]-(comment1211),\n"
                   /*
                    * Comment-Post/Comment
                    */
                    + "(post11)<-[:" + Rels.REPLY_OF + "]-(comment111),\n"
                    + "(post12)<-[:" + Rels.REPLY_OF + "]-(comment121)<-[:" + Rels.REPLY_OF + "]-(comment1211),\n"
                    + "(post21)<-[:" + Rels.REPLY_OF + "]-(comment211)<-[:" + Rels.REPLY_OF + "]-(comment2111)<-[:" + Rels.REPLY_OF + "]-(comment21111),\n"
                    + "(post31)<-[:" + Rels.REPLY_OF + "]-(comment311)";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "friend1", TestPersons.friend1(),
                    "friend2", TestPersons.friend2(),
                    "stranger3", TestPersons.stranger3(),
                    "friendfriend4", TestPersons.friendfriend4(),
                    // Posts
                    "post01", TestPosts.post01(),
                    "post11", TestPosts.post11(),
                    "post12", TestPosts.post12(),
                    "post21", TestPosts.post21(),
                    "post31", TestPosts.post31(),
                    // Comments
                    "comment111", TestComments.comment111(),
                    "comment121", TestComments.comment121(),
                    "comment1211", TestComments.comment1211(),
                    "comment211", TestComments.comment211(),
                    "comment2111", TestComments.comment2111(),
                    "comment21111", TestComments.comment21111(),
                    "comment311", TestComments.comment311()
            );
        }

        protected static class TestPersons {
            protected static Map<String, Object> person0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 0L);
                params.put(Person.FIRST_NAME, "person");
                params.put(Person.LAST_NAME, "zero");
                return params;
            }

            protected static Map<String, Object> friend1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "one");
                return params;
            }

            protected static Map<String, Object> friend2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "two");
                return params;
            }

            protected static Map<String, Object> stranger3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "stranger");
                params.put(Person.LAST_NAME, "three");
                return params;
            }

            protected static Map<String, Object> friendfriend4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "friendfriend");
                params.put(Person.LAST_NAME, "four");
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> post01() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1L);
                params.put(Message.CONTENT, "P01");
                params.put(Message.CREATION_DATE, 3L);
                return params;
            }

            protected static Map<String, Object> post11() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 11L);
                params.put(Message.CONTENT, "P11");
                params.put(Message.CREATION_DATE, 11L);
                return params;
            }

            protected static Map<String, Object> post12() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 12L);
                params.put(Message.CONTENT, "P12");
                params.put(Message.CREATION_DATE, 4L);
                return params;
            }

            protected static Map<String, Object> post21() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 21L);
                params.put(Message.CONTENT, "P21");
                params.put(Message.CREATION_DATE, 6L);
                return params;
            }

            protected static Map<String, Object> post31() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 31L);
                params.put(Message.CONTENT, "P31");
                params.put(Message.CREATION_DATE, 1L);
                return params;
            }
        }

        protected static class TestComments {
            protected static Map<String, Object> comment111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 111L);
                params.put(Message.CREATION_DATE, 12L);
                params.put(Message.CONTENT, "C111");
                return params;
            }

            protected static Map<String, Object> comment121() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 121L);
                params.put(Message.CREATION_DATE, 5L);
                params.put(Message.CONTENT, "C121");
                return params;
            }

            protected static Map<String, Object> comment1211() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 1211L);
                params.put(Message.CREATION_DATE, 10L);
                params.put(Message.CONTENT, "C1211");
                return params;
            }

            protected static Map<String, Object> comment211() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 211L);
                params.put(Message.CREATION_DATE, 7L);
                params.put(Message.CONTENT, "C211");
                return params;
            }

            protected static Map<String, Object> comment2111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 2111L);
                params.put(Message.CREATION_DATE, 8L);
                params.put(Message.CONTENT, "C2111");
                return params;
            }

            protected static Map<String, Object> comment21111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 21111L);
                params.put(Message.CREATION_DATE, 9L);
                params.put(Message.CONTENT, "C21111");
                return params;
            }

            protected static Map<String, Object> comment311() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 311L);
                params.put(Message.CREATION_DATE, 4L);
                params.put(Message.CONTENT, "C311");
                return params;
            }
        }
    }

    public static class Query10GraphMaker implements QueryGraphMaker {
        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (person0:" + Nodes.Person + " {person0}),\n"
                    + " (f1:" + Nodes.Person + " {f1}),\n"
                    + " (f2:" + Nodes.Person + " {f2}),\n"
                    + " (ff11:" + Nodes.Person + " {ff11}),\n"
                    + " (ff12:" + Nodes.Person + " {ff12}),\n"
                    + " (ff21:" + Nodes.Person + " {ff21}),\n"
                    + " (ff22:" + Nodes.Person + " {ff22}),\n"
                    + " (ff23:" + Nodes.Person + " {ff23}),\n"
                   /*
                    * Posts
                    */
                    + " (post111:" + Nodes.Post + " {post111}),\n"
                    + " (post112:" + Nodes.Post + " {post112}),\n"
                    + " (post113:" + Nodes.Post + " {post113}),\n"
                    + " (post121:" + Nodes.Post + " {post121}),\n"
                    + " (post211:" + Nodes.Post + " {post211}),\n"
                    + " (post212:" + Nodes.Post + " {post212}),\n"
                    + " (post213:" + Nodes.Post + " {post213}),\n"
                   /*
                    * Cities
                    */
                    + " (city0:" + Place.Type.City + " {city0}),\n"
                    + " (city1:" + Place.Type.City + " {city1}),\n"
                   /*
                    * Tags
                    */
                    + " (uncommonTag1:" + Nodes.Tag + " {uncommonTag1}),\n"
                    + " (uncommonTag2:" + Nodes.Tag + " {uncommonTag2}),\n"
                    + " (uncommonTag3:" + Nodes.Tag + " {uncommonTag3}),\n"
                    + " (commonTag4:" + Nodes.Tag + " {commonTag4}),\n"
                    + " (commonTag5:" + Nodes.Tag + " {commonTag5}),\n"
                    + " (commonTag6:" + Nodes.Tag + " {commonTag6}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                    + "(person0)-[:" + Rels.KNOWS + "]->(f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                    + "(f2)<-[:" + Rels.KNOWS + "]-(f1)-[:" + Rels.KNOWS + "]->(ff12),\n"
                    + "(person0)-[:" + Rels.KNOWS + "]->(f2)-[:" + Rels.KNOWS + "]->(ff21),\n"
                    + "(f2)-[:" + Rels.KNOWS + "]->(ff22),\n"
                    + "(f2)-[:" + Rels.KNOWS + "]->(ff23),\n"
                   /*
                    * Person-City
                    */
                    + " (person0)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (f1)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (f2)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (ff11)-[:" + Rels.IS_LOCATED_IN + "]->(city1),\n"
                    + " (ff12)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (ff21)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (ff22)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                    + " (ff23)-[:" + Rels.IS_LOCATED_IN + "]->(city0),\n"
                   /*
                    * Person-Post
                    */
                    + "(ff11)<-[:" + Rels.HAS_CREATOR + "]-(post111),\n"
                    + "(ff11)<-[:" + Rels.HAS_CREATOR + "]-(post112),\n"
                    + "(ff11)<-[:" + Rels.HAS_CREATOR + "]-(post113),\n"
                    + "(ff12)<-[:" + Rels.HAS_CREATOR + "]-(post121),\n"
                    + "(ff21)<-[:" + Rels.HAS_CREATOR + "]-(post211),\n"
                    + "(ff21)<-[:" + Rels.HAS_CREATOR + "]-(post212),\n"
                    + "(ff21)<-[:" + Rels.HAS_CREATOR + "]-(post213),\n"
                   /*
                    * Person-Tag
                    */
                    + "(person0)-[:" + Rels.HAS_INTEREST + "]->(commonTag4),\n"
                    + "(person0)-[:" + Rels.HAS_INTEREST + "]->(commonTag5),\n"
                    + "(person0)-[:" + Rels.HAS_INTEREST + "]->(commonTag6),\n"
                   /*
                    * Post-Tag
                    */
                    + "(post111)-[:" + Rels.HAS_TAG + "]->(uncommonTag2),\n"
                    + "(post112)-[:" + Rels.HAS_TAG + "]->(uncommonTag2),\n"
                    + "(post113)-[:" + Rels.HAS_TAG + "]->(commonTag5),\n"
                    + "(post113)-[:" + Rels.HAS_TAG + "]->(commonTag6),\n"
                    + "(post211)-[:" + Rels.HAS_TAG + "]->(uncommonTag1),\n"
                    + "(post212)-[:" + Rels.HAS_TAG + "]->(uncommonTag3),\n"
                    + "(post212)-[:" + Rels.HAS_TAG + "]->(commonTag4),\n"
                    + "(post213)-[:" + Rels.HAS_TAG + "]->(uncommonTag3)";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "ff11", TestPersons.ff11(),
                    "ff12", TestPersons.ff12(),
                    "ff21", TestPersons.ff21(),
                    "ff22", TestPersons.ff22(),
                    "ff23", TestPersons.ff23(),
                    // Posts
                    "post111", TestPosts.post111(),
                    "post112", TestPosts.post112(),
                    "post113", TestPosts.post113(),
                    "post121", TestPosts.post121(),
                    "post211", TestPosts.post211(),
                    "post212", TestPosts.post212(),
                    "post213", TestPosts.post213(),
                    // Cities
                    "city0", TestCities.city0(),
                    "city1", TestCities.city1(),
                    // Tags
                    "uncommonTag1", TestTags.uncommonTag1(),
                    "uncommonTag2", TestTags.uncommonTag2(),
                    "uncommonTag3", TestTags.uncommonTag3(),
                    "commonTag4", TestTags.commonTag4(),
                    "commonTag5", TestTags.commonTag5(),
                    "commonTag6", TestTags.commonTag6()
            );
        }

        protected static class TestCities {
            protected static Map<String, Object> city0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "city0");
                return params;
            }

            protected static Map<String, Object> city1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "city1");
                return params;
            }
        }

        protected static class TestPersons {
            protected static Map<String, Object> person0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 0L);
                params.put(Person.FIRST_NAME, "person");
                params.put(Person.LAST_NAME, "zero");
                params.put(Person.GENDER, "male");
                int birthdayMonth = 6;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }

            protected static Map<String, Object> f1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "one");
                params.put(Person.GENDER, "male");
                int birthdayMonth = 2;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "two");
                params.put(Person.GENDER, "male");
                int birthdayMonth = 2;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }

            protected static Map<String, Object> ff11() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 11L);
                params.put(Person.FIRST_NAME, "friendfriend");
                params.put(Person.LAST_NAME, "one one");
                params.put(Person.GENDER, "female");
                int birthdayMonth = 2;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }

            protected static Map<String, Object> ff12() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 12L);
                params.put(Person.FIRST_NAME, "friendfriend");
                params.put(Person.LAST_NAME, "one two");
                params.put(Person.GENDER, "male");
                int birthdayMonth = 2;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }

            protected static Map<String, Object> ff21() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 21L);
                params.put(Person.FIRST_NAME, "friendfriend");
                params.put(Person.LAST_NAME, "two one");
                params.put(Person.GENDER, "male");
                int birthdayMonth = 2;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }

            protected static Map<String, Object> ff22() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 22L);
                params.put(Person.FIRST_NAME, "friendfriend");
                params.put(Person.LAST_NAME, "two two");
                params.put(Person.GENDER, "male");
                int birthdayMonth = 2;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }

            protected static Map<String, Object> ff23() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 23L);
                params.put(Person.FIRST_NAME, "friendfriend");
                params.put(Person.LAST_NAME, "two three");
                params.put(Person.GENDER, "male");
                int birthdayMonth = 3;
                Calendar c = Calendar.getInstance();
                c.clear();
                c.set(2010, birthdayMonth, 1);
                params.put(Person.BIRTHDAY, c.getTime().getTime());
                params.put(Person.BIRTHDAY_MONTH, c.get(Calendar.MONTH));
                params.put(Person.BIRTHDAY_DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH));
                return params;
            }
        }

        protected static class TestPosts {
            protected static Map<String, Object> post111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 111L);
                params.put(Message.CONTENT, "P111");
                return params;
            }

            protected static Map<String, Object> post112() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 112L);
                params.put(Message.CONTENT, "P112");
                return params;
            }

            protected static Map<String, Object> post113() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 113L);
                params.put(Message.CONTENT, "P113");
                return params;
            }

            protected static Map<String, Object> post121() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 121L);
                params.put(Message.CONTENT, "P121");
                return params;
            }

            protected static Map<String, Object> post211() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 211L);
                params.put(Message.CONTENT, "P211");
                return params;
            }

            protected static Map<String, Object> post212() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 212L);
                params.put(Message.CONTENT, "P212");
                return params;
            }

            protected static Map<String, Object> post213() {
                Map<String, Object> params = new HashMap<>();
                params.put(Message.ID, 213L);
                params.put(Message.CONTENT, "P213");
                return params;
            }
        }

        protected static class TestTags {
            protected static Map<String, Object> uncommonTag1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.URI, 1L);
                params.put(Tag.NAME, "uncommon tag 1");
                return params;
            }

            protected static Map<String, Object> uncommonTag2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.URI, 2L);
                params.put(Tag.NAME, "uncommon tag 2");
                return params;
            }

            protected static Map<String, Object> uncommonTag3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.URI, 3L);
                params.put(Tag.NAME, "common tag 3");
                return params;
            }

            protected static Map<String, Object> commonTag4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.URI, 4L);
                params.put(Tag.NAME, "common tag 4");
                return params;
            }

            protected static Map<String, Object> commonTag5() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.URI, 5L);
                params.put(Tag.NAME, "common tag 5");
                return params;
            }

            protected static Map<String, Object> commonTag6() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.URI, 6L);
                params.put(Tag.NAME, "common tag 6");
                return params;
            }
        }
    }

    public static class Query11GraphMaker implements QueryGraphMaker {

        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (person0:" + Nodes.Person + " {person0}),\n"
                    + " (f1:" + Nodes.Person + " {f1}),\n"
                    + " (f2:" + Nodes.Person + " {f2}),\n"
                    + " (stranger3:" + Nodes.Person + " {stranger3}),\n"
                    + " (ff11:" + Nodes.Person + " {ff11}),\n"
                   /*
                    * Companies
                    */
                    + " (company0:" + Organisation.Type.Company + " {company0}),\n"
                    + " (company1:" + Organisation.Type.Company + " {company1}),\n"
                    + " (company2:" + Organisation.Type.Company + " {company2}),\n"
                   /*
                    * Countries
                    */
                    + " (country0:" + Place.Type.Country + " {country0}),\n"
                    + " (country1:" + Place.Type.Country + " {country1}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                    + "(person0)-[:" + Rels.KNOWS + "]->(f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                    + " (f1)-[:" + Rels.KNOWS + "]->(f2),\n"
                    + "(person0)-[:" + Rels.KNOWS + "]->(f2),\n"
                   /*
                    * Person-Company
                    */
                    + "(f1)-[:" + Rels.WORKS_AT + " {f1WorkedAtCompany0}]->(company0),\n"
                    + "(f1)-[:" + Rels.WORKS_AT + " {f1WorkedAtCompany1}]->(company1),\n"
                    + "(f2)-[:" + Rels.WORKS_AT + " {f2WorkedAtCompany2}]->(company2),\n"
                    + "(ff11)-[:" + Rels.WORKS_AT + " {ff11WorkedAtCompany0}]->(company0),\n"
                    + "(stranger3)-[:" + Rels.WORKS_AT + " {stranger3WorkedAtCompany2}]->(company2),\n"
                   /*
                    * Company-Country
                    */
                    + "(company0)-[:" + Rels.IS_LOCATED_IN + "]->(country0),\n"
                    + "(company1)-[:" + Rels.IS_LOCATED_IN + "]->(country1),\n"
                    + "(company2)-[:" + Rels.IS_LOCATED_IN + "]->(country0)";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "stranger3", TestPersons.stranger3(),
                    "ff11", TestPersons.ff11(),
                    // Companies
                    "company0", TestCompanies.company0(),
                    "company1", TestCompanies.company1(),
                    "company2", TestCompanies.company2(),
                    // -WorkedAt-
                    "f1WorkedAtCompany0", TestWorkedAt.f1WorkedAtCompany0(),
                    "f1WorkedAtCompany1", TestWorkedAt.f1WorkedAtCompany1(),
                    "f2WorkedAtCompany2", TestWorkedAt.f2WorkedAtCompany2(),
                    "ff11WorkedAtCompany0", TestWorkedAt.ff11WorkedAtCompany0(),
                    "stranger3WorkedAtCompany2", TestWorkedAt.stranger3WorkedAtCompany2(),
                    // Countries
                    "country0", TestCountries.country0(),
                    "country1", TestCountries.country1()
            );
        }

        protected static class TestPersons {
            protected static Map<String, Object> person0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 0L);
                params.put(Person.FIRST_NAME, "person");
                params.put(Person.LAST_NAME, "zero");
                return params;
            }

            protected static Map<String, Object> f1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "one");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "friend");
                params.put(Person.LAST_NAME, "two");
                return params;
            }

            protected static Map<String, Object> stranger3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "stranger");
                params.put(Person.LAST_NAME, "three");
                return params;
            }

            protected static Map<String, Object> ff11() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 11L);
                params.put(Person.FIRST_NAME, "friend friend");
                params.put(Person.LAST_NAME, "one one");
                return params;
            }
        }

        protected static class TestCompanies {
            protected static Map<String, Object> company0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "company zero");
                return params;
            }

            protected static Map<String, Object> company1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "company one");
                return params;
            }

            protected static Map<String, Object> company2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Organisation.NAME, "company two");
                return params;
            }
        }

        protected static class TestWorkedAt {
            protected static Map<String, Object> f1WorkedAtCompany0() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 2);
                return params;
            }

            protected static Map<String, Object> f1WorkedAtCompany1() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 4);
                return params;
            }

            protected static Map<String, Object> f2WorkedAtCompany2() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 5);
                return params;
            }

            protected static Map<String, Object> ff11WorkedAtCompany0() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 3);
                return params;
            }

            protected static Map<String, Object> stranger3WorkedAtCompany2() {
                Map<String, Object> params = new HashMap<>();
                params.put(WorksAt.WORK_FROM, 1);
                return params;
            }
        }

        protected static class TestCountries {
            protected static Map<String, Object> country0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "country0");
                return params;
            }

            protected static Map<String, Object> country1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Place.NAME, "country1");
                return params;
            }
        }
    }

    public static class Query12GraphMaker implements QueryGraphMaker {

        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (person0:" + Nodes.Person + " {person0}),\n"
                    + " (f1:" + Nodes.Person + " {f1}),\n"
                    + " (f2:" + Nodes.Person + " {f2}),\n"
                    + " (f3:" + Nodes.Person + " {f3}),\n"
                    + " (f4:" + Nodes.Person + " {f4}),\n"
                    + " (ff11:" + Nodes.Person + " {ff11}),\n"
                   /*
                    * TagClass
                    */
                    + " (tc1:" + Nodes.TagClass + " {tc1}),\n"
                    + " (tc11:" + Nodes.TagClass + " {tc11}),\n"
                    + " (tc12:" + Nodes.TagClass + " {tc12}),\n"
                    + " (tc121:" + Nodes.TagClass + " {tc121}),\n"
                    + " (tc1211:" + Nodes.TagClass + " {tc1211}),\n"
                    + " (tc2:" + Nodes.TagClass + " {tc2}),\n"
                    + " (tc21:" + Nodes.TagClass + " {tc21}),\n"
                   /*
                    * Tag
                    */
                    + " (t11:" + Nodes.Tag + " {t11}),\n"
                    + " (t111:" + Nodes.Tag + " {t111}),\n"
                    + " (t112:" + Nodes.Tag + " {t112}),\n"
                    + " (t12111:" + Nodes.Tag + " {t12111}),\n"
                    + " (t21:" + Nodes.Tag + " {t21}),\n"
                    + " (t211:" + Nodes.Tag + " {t211}),\n"
                   /*
                    * Post
                    */
                    + " (p11:" + Nodes.Post + " {" + Message.CONTENT + ":'p11'}),\n"
                    + " (p111:" + Nodes.Post + " {" + Message.CONTENT + ":'p111'}),\n"
                    + " (p112:" + Nodes.Post + " {" + Message.CONTENT + ":'p112'}),\n"
                    + " (p12111:" + Nodes.Post + " {" + Message.CONTENT + ":'p12111'}),\n"
                    + " (p21:" + Nodes.Post + " {" + Message.CONTENT + ":'p21'}),\n"
                    + " (p211:" + Nodes.Post + " {" + Message.CONTENT + ":'p211'}),\n"
                   /*
                    * Comment
                    */
                    + " (c111:" + Nodes.Comment + " {" + Message.CONTENT + ":'c111'}),\n"
                    + " (c1111:" + Nodes.Comment + " {" + Message.CONTENT + ":'c1111'}),\n"
                    + " (c11111:" + Nodes.Comment + " {" + Message.CONTENT + ":'c11111'}),\n"
                    + " (c111111:" + Nodes.Comment + " {" + Message.CONTENT + ":'c111111'}),\n"
                    + " (c11112:" + Nodes.Comment + " {" + Message.CONTENT + ":'c11112'}),\n"
                    + " (c1112:" + Nodes.Comment + " {" + Message.CONTENT + ":'c1112'}),\n"
                    + " (c1121:" + Nodes.Comment + " {" + Message.CONTENT + ":'c1121'}),\n"
                    + " (c11211:" + Nodes.Comment + " {" + Message.CONTENT + ":'c11211'}),\n"
                    + " (c112111:" + Nodes.Comment + " {" + Message.CONTENT + ":'c112111'}),\n"
                    + " (c112112:" + Nodes.Comment + " {" + Message.CONTENT + ":'c112112'}),\n"
                    + " (c121111:" + Nodes.Comment + " {" + Message.CONTENT + ":'c121111'}),\n"
                    + " (c211:" + Nodes.Comment + " {" + Message.CONTENT + ":'c211'}),\n"
                    + " (c2111:" + Nodes.Comment + " {" + Message.CONTENT + ":'c2111'}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                    + "(person0)-[:" + Rels.KNOWS + "]->(f1),\n"
                    + "(person0)-[:" + Rels.KNOWS + "]->(f2),\n"
                    + "(person0)-[:" + Rels.KNOWS + "]->(f3),\n"
                    + "(person0)-[:" + Rels.KNOWS + "]->(f4),\n"
                    + "(f1)-[:" + Rels.KNOWS + "]->(ff11),\n"
                   /*
                    * Person-Comment
                    */
                    + "(f1)<-[:" + Rels.HAS_CREATOR + "]-(c111111),\n"
                    + "(f1)<-[:" + Rels.HAS_CREATOR + "]-(c1111),\n"
                    + "(f1)<-[:" + Rels.HAS_CREATOR + "]-(c11211),\n"
                    + "(f1)<-[:" + Rels.HAS_CREATOR + "]-(c121111),\n"
                    + "(f2)<-[:" + Rels.HAS_CREATOR + "]-(c1112),\n"
                    + "(f2)<-[:" + Rels.HAS_CREATOR + "]-(c112111),\n"
                    + "(f3)<-[:" + Rels.HAS_CREATOR + "]-(c112112),\n"
                    + "(f3)<-[:" + Rels.HAS_CREATOR + "]-(c111),\n"
                    + "(f3)<-[:" + Rels.HAS_CREATOR + "]-(c211),\n"
                    + "(f3)<-[:" + Rels.HAS_CREATOR + "]-(c2111),\n"
                    + "(ff11)<-[:" + Rels.HAS_CREATOR + "]-(c11112),\n"
                    + "(ff11)<-[:" + Rels.HAS_CREATOR + "]-(c11111),\n"
                    + "(ff11)<-[:" + Rels.HAS_CREATOR + "]-(c1121),\n"
                   /*
                    * Comment-Comment
                    */
                    + "(c1111)<-[:" + Rels.REPLY_OF + "]-(c11111),\n"
                    + "(c1111)<-[:" + Rels.REPLY_OF + "]-(c11112),\n"
                    + "(c11111)<-[:" + Rels.REPLY_OF + "]-(c111111),\n"
                    + "(c1121)<-[:" + Rels.REPLY_OF + "]-(c11211),\n"
                    + "(c11211)<-[:" + Rels.REPLY_OF + "]-(c112111),\n"
                    + "(c11211)<-[:" + Rels.REPLY_OF + "]-(c112112),\n"
                   /*
                    * Comment-Post
                    */
                    + "(p11)<-[:" + Rels.REPLY_OF + "]-(c111),\n"
                    + "(p111)<-[:" + Rels.REPLY_OF + "]-(c1111),\n"
                    + "(p111)<-[:" + Rels.REPLY_OF + "]-(c1112),\n"
                    + "(p112)<-[:" + Rels.REPLY_OF + "]-(c1121),\n"
                    + "(p12111)<-[:" + Rels.REPLY_OF + "]-(c121111),\n"
                    + "(p21)<-[:" + Rels.REPLY_OF + "]-(c211),\n"
                    + "(p211)<-[:" + Rels.REPLY_OF + "]-(c2111),\n"
                   /*
                    * Post-Tag
                    */
                    + "(p11)-[:" + Rels.HAS_TAG + "]->(t11),\n"
                    + "(p11)-[:" + Rels.HAS_TAG + "]->(t12111),\n"
                    + "(p111)-[:" + Rels.HAS_TAG + "]->(t111),\n"
                    + "(p112)-[:" + Rels.HAS_TAG + "]->(t112),\n"
                    + "(p12111)-[:" + Rels.HAS_TAG + "]->(t12111),\n"
                    + "(p12111)-[:" + Rels.HAS_TAG + "]->(t21),\n"
                    + "(p21)-[:" + Rels.HAS_TAG + "]->(t21),\n"
                    + "(p211)-[:" + Rels.HAS_TAG + "]->(t211),\n"
                   /*
                    * Comment-Tag
                    */
                    + "(c1111)-[:" + Rels.HAS_TAG + "]->(t112),\n"
                    + "(c1121)-[:" + Rels.HAS_TAG + "]->(t11),\n"
                    + "(c11211)-[:" + Rels.HAS_TAG + "]->(t12111),\n"
                   /*
                    * Tag-TagClass
                    */
                    + "(tc1)<-[:" + Rels.HAS_TYPE + "]-(t11),\n"
                    + "(tc11)<-[:" + Rels.HAS_TYPE + "]-(t111),\n"
                    + "(tc11)<-[:" + Rels.HAS_TYPE + "]-(t112),\n"
                    + "(tc1211)<-[:" + Rels.HAS_TYPE + "]-(t12111),\n"
                    + "(tc2)<-[:" + Rels.HAS_TYPE + "]-(t21),\n"
                    + "(tc21)<-[:" + Rels.HAS_TYPE + "]-(t211),\n"
                   /*
                    * TagClass-TagClass
                    */
                    + "(tc11)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc1),\n"
                    + "(tc12)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc1),\n"
                    + "(tc121)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc12),\n"
                    + "(tc1211)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc121),\n"
                    + "(tc21)-[:" + Rels.IS_SUBCLASS_OF + "]->(tc2)\n";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map(
                    // Persons
                    "person0", TestPersons.person0(),
                    "f1", TestPersons.f1(),
                    "f2", TestPersons.f2(),
                    "f3", TestPersons.f3(),
                    "f4", TestPersons.f4(),
                    "ff11", TestPersons.ff11(),
                    // Tags
                    "t11", TestTags.t11(),
                    "t111", TestTags.t111(),
                    "t112", TestTags.t112(),
                    "t12111", TestTags.t12111(),
                    "t21", TestTags.t21(),
                    "t211", TestTags.t211(),
                    // TagClasses
                    "tc1", TestTagClasses.tc1(),
                    "tc11", TestTagClasses.tc11(),
                    "tc12", TestTagClasses.tc12(),
                    "tc121", TestTagClasses.tc121(),
                    "tc1211", TestTagClasses.tc1211(),
                    "tc2", TestTagClasses.tc2(),
                    "tc21", TestTagClasses.tc21()
            );
        }

        protected static class TestPersons {
            protected static Map<String, Object> person0() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 0L);
                params.put(Person.FIRST_NAME, "person");
                params.put(Person.LAST_NAME, "0");
                return params;
            }

            protected static Map<String, Object> f1() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 1L);
                params.put(Person.FIRST_NAME, "f");
                params.put(Person.LAST_NAME, "1");
                return params;
            }

            protected static Map<String, Object> f2() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 2L);
                params.put(Person.FIRST_NAME, "f");
                params.put(Person.LAST_NAME, "2");
                return params;
            }

            protected static Map<String, Object> f3() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 3L);
                params.put(Person.FIRST_NAME, "f");
                params.put(Person.LAST_NAME, "3");
                return params;
            }

            protected static Map<String, Object> f4() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 4L);
                params.put(Person.FIRST_NAME, "f");
                params.put(Person.LAST_NAME, "4");
                return params;
            }

            protected static Map<String, Object> ff11() {
                Map<String, Object> params = new HashMap<>();
                params.put(Person.ID, 11L);
                params.put(Person.FIRST_NAME, "ff");
                params.put(Person.LAST_NAME, "11");
                return params;
            }
        }

        protected static class TestTags {
            protected static Map<String, Object> t11() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag11");
                return params;
            }

            protected static Map<String, Object> t111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag111");
                return params;
            }

            protected static Map<String, Object> t112() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag112");
                return params;
            }

            protected static Map<String, Object> t12111() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag12111");
                return params;
            }

            protected static Map<String, Object> t21() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag21");
                return params;
            }

            protected static Map<String, Object> t211() {
                Map<String, Object> params = new HashMap<>();
                params.put(Tag.NAME, "tag211");
                return params;
            }
        }

        protected static class TestTagClasses {
            protected static Map<String, Object> tc1() {
                Map<String, Object> params = new HashMap<>();
                params.put(TagClass.URI, 1L);
                params.put(TagClass.NAME, "1");
                return params;
            }

            protected static Map<String, Object> tc11() {
                Map<String, Object> params = new HashMap<>();
                params.put(TagClass.URI, 11L);
                params.put(TagClass.NAME, "11");
                return params;
            }

            protected static Map<String, Object> tc12() {
                Map<String, Object> params = new HashMap<>();
                params.put(TagClass.URI, 12L);
                params.put(TagClass.NAME, "12");
                return params;
            }

            protected static Map<String, Object> tc121() {
                Map<String, Object> params = new HashMap<>();
                params.put(TagClass.URI, 121L);
                params.put(TagClass.NAME, "121");
                return params;
            }

            protected static Map<String, Object> tc1211() {
                Map<String, Object> params = new HashMap<>();
                params.put(TagClass.URI, 1211L);
                params.put(TagClass.NAME, "1211");
                return params;
            }

            protected static Map<String, Object> tc2() {
                Map<String, Object> params = new HashMap<>();
                params.put(TagClass.URI, 2L);
                params.put(TagClass.NAME, "2");
                return params;
            }

            protected static Map<String, Object> tc21() {
                Map<String, Object> params = new HashMap<>();
                params.put(TagClass.URI, 21L);
                params.put(TagClass.NAME, "21");
                return params;
            }
        }
    }

    public static class Query13GraphMaker implements QueryGraphMaker {

        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + " (p0:" + Nodes.Person + " {" + Person.ID + ":0}),\n"
                    + " (p1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                    + " (p2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                    + " (p3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                    + " (p4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                    + " (p5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"
                    + " (p6:" + Nodes.Person + " {" + Person.ID + ":6}),\n"
                    + " (p7:" + Nodes.Person + " {" + Person.ID + ":7}),\n"
                    + " (p8:" + Nodes.Person + " {" + Person.ID + ":8}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                    + "(p0)-[:" + Rels.KNOWS + "]->(p1),\n"
                    + "(p1)-[:" + Rels.KNOWS + "]->(p3),\n"
                    + "(p1)<-[:" + Rels.KNOWS + "]-(p2),\n"
                    + "(p3)-[:" + Rels.KNOWS + "]->(p2),\n"
                    + "(p2)<-[:" + Rels.KNOWS + "]-(p4),\n"
                    + "(p4)-[:" + Rels.KNOWS + "]->(p7),\n"
                    + "(p4)-[:" + Rels.KNOWS + "]->(p6),\n"
                    + "(p6)<-[:" + Rels.KNOWS + "]-(p5)";
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map();
        }
    }

    public static class Query14GraphMaker implements QueryGraphMaker {

        @Override
        public String queryString() {
            return "CREATE\n"
                    + "\n// --- NODES ---\n\n"
                   /*
                    * Persons
                    */
                    + "(p0:" + Nodes.Person + " {" + Person.ID + ":0}),\n"
                    + "(p1:" + Nodes.Person + " {" + Person.ID + ":1}),\n"
                    + "(p2:" + Nodes.Person + " {" + Person.ID + ":2}),\n"
                    + "(p3:" + Nodes.Person + " {" + Person.ID + ":3}),\n"
                    + "(p4:" + Nodes.Person + " {" + Person.ID + ":4}),\n"
                    + "(p5:" + Nodes.Person + " {" + Person.ID + ":5}),\n"
                    + "(p6:" + Nodes.Person + " {" + Person.ID + ":6}),\n"
                    + "(p7:" + Nodes.Person + " {" + Person.ID + ":7}),\n"
                    + "(p8:" + Nodes.Person + " {" + Person.ID + ":8}),\n"
                   /*
                    * Posts
                    */
                    + "(p0Post1:" + Nodes.Post + " {" + Message.ID + ":0}),\n"
                    + "(p1Post1:" + Nodes.Post + " {" + Message.ID + ":1}),\n"
                    + "(p3Post1:" + Nodes.Post + " {" + Message.ID + ":2}),\n"
                    + "(p5Post1:" + Nodes.Post + " {" + Message.ID + ":3}),\n"
                    + "(p6Post1:" + Nodes.Post + " {" + Message.ID + ":4}),\n"
                    + "(p7Post1:" + Nodes.Post + " {" + Message.ID + ":5}),\n"
                   /*
                    * Comments
                    */
                    + "(p0Comment1:" + Nodes.Comment + " {" + Message.ID + ":6}),\n"
                    + "(p1Comment1:" + Nodes.Comment + " {" + Message.ID + ":7}),\n"
                    + "(p1Comment2:" + Nodes.Comment + " {" + Message.ID + ":8}),\n"
                    + "(p4Comment1:" + Nodes.Comment + " {" + Message.ID + ":9}),\n"
                    + "(p4Comment2:" + Nodes.Comment + " {" + Message.ID + ":10}),\n"
                    + "(p5Comment1:" + Nodes.Comment + " {" + Message.ID + ":11}),\n"
                    + "(p5Comment2:" + Nodes.Comment + " {" + Message.ID + ":12}),\n"
                    + "(p7Comment1:" + Nodes.Comment + " {" + Message.ID + ":13}),\n"
                    + "(p8Comment1:" + Nodes.Comment + " {" + Message.ID + ":14}),\n"
                    + "(p8Comment2:" + Nodes.Comment + " {" + Message.ID + ":15}),\n"

                    + "\n// --- RELATIONSHIPS ---\n\n"
                   /*
                    * Person-Person
                    */
                    + "(p0)-[:" + Rels.KNOWS + "]->(p1),\n"
                    + "(p1)-[:" + Rels.KNOWS + "]->(p3),\n"
                    + "(p1)<-[:" + Rels.KNOWS + "]-(p2),\n"
                    + "(p1)<-[:" + Rels.KNOWS + "]-(p7),\n"
                    + "(p3)-[:" + Rels.KNOWS + "]->(p2),\n"
                    + "(p2)<-[:" + Rels.KNOWS + "]-(p4),\n"
                    + "(p4)-[:" + Rels.KNOWS + "]->(p7),\n"
                    + "(p4)-[:" + Rels.KNOWS + "]->(p8),\n"
                    + "(p4)-[:" + Rels.KNOWS + "]->(p6),\n"
                    + "(p6)<-[:" + Rels.KNOWS + "]-(p5),\n"
                    + "(p8)<-[:" + Rels.KNOWS + "]-(p5),\n"
                   /*
                    * Person-Post
                    */
                    + "(p0)<-[:" + Rels.HAS_CREATOR + "]-(p0Post1),\n"
                    + "(p1)<-[:" + Rels.HAS_CREATOR + "]-(p1Post1),\n"
                    + "(p3)<-[:" + Rels.HAS_CREATOR + "]-(p3Post1),\n"
                    + "(p5)<-[:" + Rels.HAS_CREATOR + "]-(p5Post1),\n"
                    + "(p6)<-[:" + Rels.HAS_CREATOR + "]-(p6Post1),\n"
                    + "(p7)<-[:" + Rels.HAS_CREATOR + "]-(p7Post1),\n"
                   /*
                    * Person-Comment
                    */
                    + "(p0)<-[:" + Rels.HAS_CREATOR + "]-(p0Comment1),\n"
                    + "(p1)<-[:" + Rels.HAS_CREATOR + "]-(p1Comment1),\n"
                    + "(p1)<-[:" + Rels.HAS_CREATOR + "]-(p1Comment2),\n"
                    + "(p4)<-[:" + Rels.HAS_CREATOR + "]-(p4Comment1),\n"
                    + "(p4)<-[:" + Rels.HAS_CREATOR + "]-(p4Comment2),\n"
                    + "(p5)<-[:" + Rels.HAS_CREATOR + "]-(p5Comment1),\n"
                    + "(p5)<-[:" + Rels.HAS_CREATOR + "]-(p5Comment2),\n"
                    + "(p7)<-[:" + Rels.HAS_CREATOR + "]-(p7Comment1),\n"
                    + "(p8)<-[:" + Rels.HAS_CREATOR + "]-(p8Comment1),\n"
                    + "(p8)<-[:" + Rels.HAS_CREATOR + "]-(p8Comment2),\n"
                   /*
                    * Comment-Post
                    */
                    + "(p1Post1)<-[:" + Rels.REPLY_OF + "]-(p0Comment1),\n"
                    + "(p0Post1)<-[:" + Rels.REPLY_OF + "]-(p1Comment1),\n"
                    + "(p0Post1)<-[:" + Rels.REPLY_OF + "]-(p1Comment2),\n"
                    + "(p3Post1)<-[:" + Rels.REPLY_OF + "]-(p4Comment1),\n"
                    + "(p7Post1)<-[:" + Rels.REPLY_OF + "]-(p4Comment2),\n"
                    + "(p5Post1)<-[:" + Rels.REPLY_OF + "]-(p5Comment1),\n"
                    + "(p6Post1)<-[:" + Rels.REPLY_OF + "]-(p8Comment1),\n"
                   /*
                    * Comment-Comment
                    */
                    + "(p7Comment1)-[:" + Rels.REPLY_OF + "]->(p4Comment2),\n"
                    + "(p8Comment2)-[:" + Rels.REPLY_OF + "]->(p4Comment1),\n"
                    + "(p5Comment2)-[:" + Rels.REPLY_OF + "]->(p8Comment2)\n"
                    ;
        }

        @Override
        public Map<String, Object> params() {
            return MapUtil.map();
        }
    }
}
