package com.ldbc.socialnet.neo4j.workload;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;

import com.ldbc.driver.util.Pair;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Cities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Companies;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Countries;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Persons;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Posts;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Universities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Rels.StudyAt;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Rels.WorksAt;
import com.ldbc.socialnet.workload.Domain;

public class TestGraph
{
    public static class Creator
    {
        /*
        September 4-8 (4 days), Sweden, New Zealand, Alex

        Friend
            Jake       Sweden
                5 September, New Zealand,  hello       *   jakePost1
                5 September, Sweden,       hej         *   jakePost2
                7 September, Sweden,       tjena           jakePost3
            Peter      Germany
                7 September, Germany,      hallo            peterPost1
            Aiya       New Zealand
                6 September, Sweden,       kia ora     *    aiyaPost1
                9 September, New Zealand,  bro              aiyaPost2
                5 September, New Zealand,  chur        *    aiyaPost3
        
        Not Friend             
            Stranger   Sweden
                2 September, Australia,    gidday           strangerPost1
                5 September, Australia,    I heart sheep    strangerPost2
        */

        public static String createGraphQuery()
        {
            return "CREATE\n"

            /*
             --- NODES ---
             */

            /*
             * Persons
             */

            + " (alex:" + Domain.Node.PERSON + " {alex}), (aiya:" + Domain.Node.PERSON + " {aiya}),\n"

            + " (jake:" + Domain.Node.PERSON + " {jake}), (peter:" + Domain.Node.PERSON + " {peter}),\n"

            + " (stranger:" + Domain.Node.PERSON + " {stranger}),\n"

            /*
            * Cities
            */

            + " (auckland:" + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + " {auckland}), (stockholm:"
                   + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + " {stockholm}),\n"

                   + " (munich:" + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + " {munich}), (melbourne:"
                   + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + " {melbourne}),\n"

                   /*
                   * Countries
                   */

                   + " (se:" + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + " {sweden}), (nz:"
                   + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + " {new_zealand}),\n"

                   + " (de:" + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + " {germany}), (au:"
                   + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + " {australia}),\n"

                   /*
                   * Universities
                   */

                   + " (aut:" + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.UNIVERSITY + " {aut}), (kth:"
                   + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.UNIVERSITY + " {kth}),\n"

                   /*
                   * Companies
                   */

                   + " (sics:" + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.COMPANY + " {sics}), (neo:"
                   + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.COMPANY + " {neo}), (hot:"
                   + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.COMPANY + " {hot}),\n"

                   /*
                   * Posts
                   */

                   + " (jakePost1:" + Domain.Node.POST + " {jakePost1}), (jakePost2:" + Domain.Node.POST
                   + " {jakePost2}), (jakePost3:" + Domain.Node.POST + " {jakePost3}),\n"

                   + " (peterPost1:" + Domain.Node.POST + " {peterPost1}), (aiyaPost1:" + Domain.Node.POST
                   + " {aiyaPost1}), (aiyaPost2:" + Domain.Node.POST + " {aiyaPost2}), (aiyaPost3:" + Domain.Node.POST
                   + " {aiyaPost3}),\n"

                   + " (strangerPost1:" + Domain.Node.POST + " {strangerPost1}), (strangerPost2:" + Domain.Node.POST
                   + " {strangerPost2}),\n"

                   /*
                   --- RELATIONSHIPS ---
                   */

                   + "\n"

                   /*
                    * City-Country
                    */

                   + " (stockholm)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(se), (auckland)-[:" + Domain.Rel.IS_LOCATED_IN
                   + "]->(nz),\n"

                   + " (munich)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(de), (melbourne)-[:" + Domain.Rel.IS_LOCATED_IN
                   + "]->(au),\n"

                   /*
                    * Organisation-Country
                    */

                   + " (neo)<-[:" + Domain.Rel.WORKS_AT + "{alexWorkAtNeo}]-(alex), (stockholm)<-[:"
                   + Domain.Rel.IS_LOCATED_IN + "]-(kth),\n"

                   + " (auckland)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(aut), (se)<-[:" + Domain.Rel.IS_LOCATED_IN
                   + "]-(sics),\n"

                   + " (nz)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(hot),\n"

                   /*
                    * Alex
                    */

                   + " (alex)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(stockholm),\n"

                   + " (se)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(neo),\n"

                   + " (kth)<-[:" + Domain.Rel.STUDY_AT + " {alexStudyAtKth}]-(alex),\n"

                   + " (aut)<-[:" + Domain.Rel.STUDY_AT + " {alexStudyAtAut}]-(alex),\n"

                   + " (sics)<-[:" + Domain.Rel.WORKS_AT + "{alexWorkAtSics}]-(alex),\n"

                   /*
                    * Aiya
                    */

                   + " (aiya)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(auckland),\n"

                   + " (hot)<-[:" + Domain.Rel.WORKS_AT + "{aiyaWorkAtHot}]-(aiya),\n"

                   /*
                    * Jake
                    */

                   + " (jake)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(stockholm),\n"

                   /*
                   * Peter
                   */

                   + " (peter)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(munich),\n"

                   /*
                    * Stranger
                    */

                   + " (stranger)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(melbourne)\n"

                   /*
                   * Person-Person
                   */

                   + "FOREACH (n IN [jake, aiya, peter]| CREATE (alex)-[:" + Domain.Rel.KNOWS + "]->(n) )\n"

                   /*
                   * Post-City
                   */

                   + "FOREACH (n IN [jakePost1]| CREATE (n)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(nz) )\n"

                   + "FOREACH (n IN [jakePost2, jakePost3]| CREATE (n)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(se) )\n"

                   + "FOREACH (n IN [aiyaPost1]| CREATE (n)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(se) )\n"

                   + "FOREACH (n IN [aiyaPost2, aiyaPost3]| CREATE (n)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(nz) )\n"

                   + "FOREACH (n IN [peterPost1]| CREATE (n)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(de) )\n"

                   + "FOREACH (n IN [strangerPost1, strangerPost2]| CREATE (n)-[:" + Domain.Rel.IS_LOCATED_IN
                   + "]->(au) )\n"

                   /*
                   * Post-Person
                   */

                   + "FOREACH (n IN [jakePost1, jakePost2, jakePost3]| CREATE (n)-[:" + Domain.Rel.HAS_CREATOR
                   + "]->(jake) )\n"

                   + "FOREACH (n IN [aiyaPost1, aiyaPost2, aiyaPost3]| CREATE (n)-[:" + Domain.Rel.HAS_CREATOR
                   + "]->(aiya) )\n"

                   + "FOREACH (n IN [peterPost1]| CREATE (n)-[:" + Domain.Rel.HAS_CREATOR + "]->(peter) )\n"

                   + "FOREACH (n IN [strangerPost1, strangerPost2]| CREATE (n)-[:" + Domain.Rel.HAS_CREATOR
                   + "]->(stranger) )\n"

            ;
        }

        public static Map<String, Object> createGraphQueryParams()
        {
            return MapUtil.map( "alex", Persons.alex(), "aiya", Persons.aiya(), "jake", Persons.jake(), "peter",
                    Persons.peter(), "stranger", Persons.stranger(), "auckland", Cities.auckland(), "stockholm",
                    Cities.stockholm(), "munich", Cities.munich(), "melbourne", Cities.melbourne(), "sweden",
                    Countries.sweden(), "new_zealand", Countries.newZealand(), "germany", Countries.germany(),
                    "australia", Countries.australia(), "aut", Universities.aut(), "kth", Universities.kth(), "sics",
                    Companies.sics(), "neo", Companies.neo(), "hot", Companies.hot(), "alexWorkAtSics",
                    WorksAt.alexWorkAtSics(), "alexWorkAtNeo", WorksAt.alexWorkAtNeo(), "aiyaWorkAtHot",
                    WorksAt.aiyaWorkAtHot(), "alexStudyAtAut", StudyAt.alexStudyAtAut(), "alexStudyAtKth",
                    StudyAt.alexStudyAtKth(), "jakePost1", Posts.jakePost1(), "jakePost2", Posts.jakePost2(),
                    "jakePost3", Posts.jakePost3(), "peterPost1", Posts.peterPost1(), "aiyaPost1", Posts.aiyaPost1(),
                    "aiyaPost2", Posts.aiyaPost2(), "aiyaPost3", Posts.aiyaPost3(), "strangerPost1",
                    Posts.strangerPost1(), "strangerPost2", Posts.strangerPost2() );
        }

        public static Iterable<String> createIndexQueries()
        {
            List<String> createIndexQueries = new ArrayList<String>();
            for ( Pair<Label, String> labelAndProperty : Domain.labelPropertyPairsToIndex() )
            {
                createIndexQueries.add( "CREATE INDEX ON :" + labelAndProperty._1() + "(" + labelAndProperty._2() + ")" );
            }
            return createIndexQueries;
        }
    }

    protected static class Rels
    {
        protected static class WorksAt
        {
            protected static Map<String, Object> alexWorkAtSics()
            {
                return MapUtil.map( Domain.WorksAt.WORK_FROM, 2010 );
            }

            protected static Map<String, Object> alexWorkAtNeo()
            {
                return MapUtil.map( Domain.WorksAt.WORK_FROM, 2012 );
            }

            protected static Map<String, Object> aiyaWorkAtHot()
            {
                return MapUtil.map( Domain.WorksAt.WORK_FROM, 2005 );
            }
        }

        protected static class StudyAt
        {
            protected static Map<String, Object> alexStudyAtAut()
            {
                return MapUtil.map( Domain.StudiesAt.CLASS_YEAR, 2006 );
            }

            protected static Map<String, Object> alexStudyAtKth()
            {
                return MapUtil.map( Domain.StudiesAt.CLASS_YEAR, 2008 );
            }
        }
    }

    protected static class Nodes
    {
        protected static class Companies
        {
            protected static Map<String, Object> sics()
            {
                return MapUtil.map( Domain.Organisation.NAME, "swedish institute of computer science" );
            }

            protected static Map<String, Object> neo()
            {
                return MapUtil.map( Domain.Place.NAME, "neo technology" );
            }

            protected static Map<String, Object> hot()
            {
                return MapUtil.map( Domain.Place.NAME, "house of travel" );
            }
        }

        protected static class Universities
        {
            protected static Map<String, Object> aut()
            {
                return MapUtil.map( Domain.Organisation.NAME, "auckland university of technology" );
            }

            protected static Map<String, Object> kth()
            {
                return MapUtil.map( Domain.Place.NAME, "royal institute of technology" );
            }
        }

        protected static class Countries
        {
            protected static Map<String, Object> sweden()
            {
                return MapUtil.map( Domain.Place.NAME, "sweden" );
            }

            protected static Map<String, Object> newZealand()
            {
                return MapUtil.map( Domain.Place.NAME, "new zealand" );
            }

            protected static Map<String, Object> germany()
            {
                return MapUtil.map( Domain.Place.NAME, "germany" );
            }

            protected static Map<String, Object> australia()
            {
                return MapUtil.map( Domain.Place.NAME, "australia" );
            }
        }

        protected static class Cities
        {

            protected static Map<String, Object> munich()
            {
                return MapUtil.map( Domain.Place.NAME, "munich" );
            }

            protected static Map<String, Object> melbourne()
            {
                return MapUtil.map( Domain.Place.NAME, "melbourne" );
            }

            protected static Map<String, Object> malmo()
            {
                return MapUtil.map( Domain.Place.NAME, "malmö" );
            }

            protected static Map<String, Object> stockholm()
            {
                return MapUtil.map( Domain.Place.NAME, "stockholm" );
            }

            protected static Map<String, Object> auckland()
            {
                return MapUtil.map( Domain.Place.NAME, "auckland" );
            }

            protected static Map<String, Object> whangarei()
            {
                return MapUtil.map( Domain.Place.NAME, "whangarei" );
            }
        }

        protected static class Persons
        {
            protected static Map<String, Object> alex()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 1L );
                params.put( Domain.Person.FIRST_NAME, "alex" );
                params.put( Domain.Person.LAST_NAME, "averbuch" );
                Calendar c = Calendar.getInstance();
                c.set( 2012, Calendar.JUNE, 6 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.JANUARY, 23 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "chrome" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "alex.averbuch@gmail.com",
                        "alex.averbuch@neotechnology.com" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english", "swedish" } );
                params.put( Domain.Person.LOCATION_IP, "192.168.42.24" );
                return params;
            }

            protected static Map<String, Object> aiya()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 2L );
                params.put( Domain.Person.FIRST_NAME, "aiya" );
                params.put( Domain.Person.LAST_NAME, "thorpe" );
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.MAY, 19 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1983, Calendar.SEPTEMBER, 8 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "safari" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "aiya.thorpe@gmail.com" } );
                params.put( Domain.Person.GENDER, "female" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english" } );
                params.put( Domain.Person.LOCATION_IP, "192.161.48.1" );
                return params;
            }

            protected static Map<String, Object> jake()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 3L );
                params.put( Domain.Person.FIRST_NAME, "jacob" );
                params.put( Domain.Person.LAST_NAME, "hansson" );
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 10 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1987, Calendar.JULY, 21 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "safari" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "jakewins@gmail.com",
                        "jake@neotechnology.com" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english", "swedish" } );
                params.put( Domain.Person.LOCATION_IP, "172.124.98.31" );
                return params;
            }

            protected static Map<String, Object> peter()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 4L );
                params.put( Domain.Person.FIRST_NAME, "peter" );
                params.put( Domain.Person.LAST_NAME, "rentschler" );
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.JANUARY, 5 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1982, Calendar.JUNE, 5 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "firefox" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "peter.rentschler@gmx.de" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english", "german" } );
                params.put( Domain.Person.LOCATION_IP, "12.24.158.11" );
                return params;
            }

            protected static Map<String, Object> stranger()
            {
                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Person.ID, 5L );
                params.put( Domain.Person.FIRST_NAME, "strange" );
                params.put( Domain.Person.LAST_NAME, "guy" );
                Calendar c = Calendar.getInstance();
                c.set( 2012, Calendar.OCTOBER, 15 );
                long creationDate = c.getTimeInMillis();
                params.put( Domain.Person.CREATION_DATE, creationDate );
                c.set( 1985, Calendar.FEBRUARY, 11 );
                long birthday = c.getTimeInMillis();
                params.put( Domain.Person.BIRTHDAY, birthday );
                params.put( Domain.Person.BROWSER_USED, "internet explorer" );
                params.put( Domain.Person.EMAIL_ADDRESSES, new String[] { "dr.strange@love.come" } );
                params.put( Domain.Person.GENDER, "male" );
                params.put( Domain.Person.LANGUAGES, new String[] { "english" } );
                params.put( Domain.Person.LOCATION_IP, "12.24.158.11" );
                return params;
            }
        }

        protected static class Posts
        {
            protected static Map<String, Object> jakePost1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "hello" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.141" );
                return params;
            }

            protected static Map<String, Object> jakePost2()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "hej" );
                params.put( Domain.Post.LANGUAGE, new String[] { "swedish" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.142" );
                return params;
            }

            protected static Map<String, Object> jakePost3()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "tjena" );
                params.put( Domain.Post.LANGUAGE, new String[] { "swedish" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.143" );
                return params;
            }

            protected static Map<String, Object> peterPost1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 7 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "hallo" );
                params.put( Domain.Post.LANGUAGE, new String[] { "german" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "firefox" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.241" );
                return params;
            }

            protected static Map<String, Object> aiyaPost1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 6 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "kia ora" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.341" );
                return params;
            }

            protected static Map<String, Object> aiyaPost2()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 9 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "bro" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.342" );
                return params;
            }

            protected static Map<String, Object> aiyaPost3()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "chur" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "safari" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.343" );
                return params;
            }

            protected static Map<String, Object> strangerPost1()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 2 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "gidday" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "internet explorer" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.441" );
                return params;
            }

            protected static Map<String, Object> strangerPost2()
            {
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.SEPTEMBER, 5 );
                long creationDate = c.getTimeInMillis();

                Map<String, Object> params = new HashMap<String, Object>();
                params.put( Domain.Post.CONTENT, "i heart sheep" );
                params.put( Domain.Post.LANGUAGE, new String[] { "english" } );
                params.put( Domain.Post.IMAGE_FILE, "some image file" );
                params.put( Domain.Post.CREATION_DATE, creationDate );
                params.put( Domain.Post.BROWSER_USED, "internet explorer" );
                params.put( Domain.Post.LOCATION_IP, "31.55.91.442" );
                return params;
            }
        }

    }
}
