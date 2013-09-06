package com.ldbc.socialnet.neo4j.workload;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;

import com.ldbc.driver.util.Pair;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Cities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Companies;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Countries;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Persons;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Nodes.Universities;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Rels.StudyAt;
import com.ldbc.socialnet.neo4j.workload.TestGraph.Rels.WorksAt;
import com.ldbc.socialnet.workload.Domain;

public class TestGraph
{
    public static class Creator
    {
        public static String createGraphQuery()
        {
            return "CREATE\n"

            + " (alex:" + Domain.Node.PERSON + " {alex}), (aiya:" + Domain.Node.PERSON + " {aiya}),\n"

            + " (auckland:" + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + " {auckland}), (stockholm:"
                   + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + " {stockholm}),\n"

                   + " (se:" + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + " {sweden}), (nz:"
                   + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + " {new_zealand}),\n"

                   + " (aut:" + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.UNIVERSITY + " {aut}), (kth:"
                   + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.UNIVERSITY + " {kth}),\n"

                   + " (sics:" + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.COMPANY + " {sics}), (neo:"
                   + Domain.Node.ORGANISATION
                   + ":"
                   + Domain.Organisation.Type.COMPANY
                   + " {neo}), (hot:"
                   + Domain.Node.ORGANISATION
                   + ":"
                   + Domain.Organisation.Type.COMPANY
                   + " {hot})\n"

                   + "WITH alex, aiya, auckland, stockholm, se, nz, aut, kth, sics, neo, hot\n"

                   + "CREATE\n"

                   + " (alex)-[:"
                   + Domain.Rel.IS_LOCATED_IN
                   + "]->(stockholm),\n"

                   // + " (se)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(neo)<-[:" +
                   // Domain.Rel.WORKS_AT
                   // + "{alexWorkAtNeo}]-(alex),\n"

                   + " (neo)<-[:" + Domain.Rel.WORKS_AT + "{alexWorkAtNeo}]-(alex),\n"

                   + " (se)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(neo),\n"

                   + " (stockholm)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(kth)<-[:" + Domain.Rel.STUDY_AT
                   + " {alexStudyAtKth}]-(alex),\n"

                   + " (auckland)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(aut)<-[:" + Domain.Rel.STUDY_AT
                   + " {alexStudyAtAut}]-(alex),\n"

                   + " (thisThing {some_field:'hi'})<-[:IS]-(se)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(sics)<-[:" + Domain.Rel.WORKS_AT
                   + "{alexWorkAtSics}]-(alex),\n"

                   + " (aiya)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(auckland),\n"

                   + " (nz)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(hot)<-[:" + Domain.Rel.WORKS_AT
                   + "{aiyaWorkAtHot}]-(aiya)\n";
        }

        public static Map<String, Object> createGraphQueryParams()
        {
            return MapUtil.map( "alex", Persons.alex(), "aiya", Persons.aiya(), "auckland", Cities.auckland(),
                    "stockholm", Cities.stockholm(), "sweden", Countries.sweden(), "new_zealand",
                    Countries.newZealand(), "aut", Universities.aut(), "kth", Universities.kth(), "sics",
                    Companies.sics(), "neo", Companies.neo(), "hot", Companies.hot(), "alexWorkAtSics",
                    WorksAt.alexWorkAtSics(), "alexWorkAtNeo", WorksAt.alexWorkAtNeo(), "aiyaWorkAtHot",
                    WorksAt.aiyaWorkAtHot(), "alexStudyAtAut", StudyAt.alexStudyAtAut(), "alexStudyAtKth",
                    StudyAt.alexStudyAtKth() );
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
        }

        protected static class Cities
        {
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
                String firstName = "alex";
                String lastName = "averbuch";
                Calendar c = Calendar.getInstance();
                c.set( 2012, Calendar.JUNE, 6 );
                long creationDate = c.getTimeInMillis();
                c.set( 1982, Calendar.JANUARY, 23 );
                long birthday = c.getTimeInMillis();
                String[] emails = { "alex.averbuch@gmail.com", "alex.averbuch@neotechnology.com" };
                String[] languages = { "english", "swedish" };

                return MapUtil.map( Domain.Person.CREATION_DATE, creationDate, Domain.Person.FIRST_NAME, firstName,
                        Domain.Person.LAST_NAME, lastName, Domain.Person.GENDER, "male", Domain.Person.BIRTHDAY,
                        birthday, Domain.Person.EMAIL_ADDRESSES, emails, Domain.Person.LANGUAGES, languages,
                        Domain.Person.BROWSER_USED, "chrome", Domain.Person.LOCATION_IP, "192.168.42.24" );
            }

            protected static Map<String, Object> aiya()
            {
                String firstName = "aiya";
                String lastName = "averbuch";
                Calendar c = Calendar.getInstance();
                c.set( 2013, Calendar.MAY, 19 );
                long creationDate = c.getTimeInMillis();
                c.set( 1983, Calendar.SEPTEMBER, 8 );
                long birthday = c.getTimeInMillis();
                String[] emails = { "aiya.thorpe@gmail.com" };
                String[] languages = { "english" };

                return MapUtil.map( Domain.Person.CREATION_DATE, creationDate, Domain.Person.FIRST_NAME, firstName,
                        Domain.Person.LAST_NAME, lastName, Domain.Person.GENDER, "female", Domain.Person.BIRTHDAY,
                        birthday, Domain.Person.EMAIL_ADDRESSES, emails, Domain.Person.LANGUAGES, languages,
                        Domain.Person.BROWSER_USED, "safari", Domain.Person.LOCATION_IP, "192.161.48.1" );
            }
        }
    }
}
