package com.ldbc.socialnet.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.ldbc.socialnet.neo4j.domain.Domain;

// TODO generator should mention cardinalities

public class Queries
{
    public static class LdbcInteractive
    {
        public static class Query1
        {
            /*
            QUERY 1
             
            Given a person’s first name, return up to 10 people with the same first name sorted by last name. 
            Persons are returned (e.g. as for a search page with top 10 shown), 
            and the information is complemented with summaries of the persons' workplaces 
            and places of study.
            
            PARAMETERS:
            
            firstName
             
            RETURN: 
            
            Person.firstName
            Person.lastName
            Person.birthday
            Person.creationDate
            Person.gender
            Person.language
            Person.browserUsed
            Person.locationIP
            Person.email
            Person-isLocatedIn->Location.name
            (Person-studyAt->University.name, Person-studyAt->.classYear, Person-studyAt->University-isLocatedIn->City.name)
            (Person-workAt->Company.name, Person-workAt->.workFrom, Person-workAt->Company-isLocatedIn->Country.name)
             */

            public static final String QUERY_TEMPLATE = String.format(

            "MATCH person:" + Domain.Node.PERSON + "\n"

            + "WHERE person." + Domain.Person.FIRST_NAME + "={ " + Domain.Person.FIRST_NAME + " }\n"

            + "WITH person\n"

            + "ORDER BY person." + Domain.Person.LAST_NAME + "\n"

            + "LIMIT 10\n"

            + "MATCH (uniCity:" + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + ")<-[:" + Domain.Rel.IS_LOCATED_IN
                    + "]-(uni:" + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.UNIVERSITY + ")<-[studyAt:"
                    + Domain.Rel.STUDY_AT + "]-(person),\n"

                    + "(company:" + Domain.Node.ORGANISATION + ":" + Domain.Organisation.Type.COMPANY + ")<-[worksAt:"
                    + Domain.Rel.WORKS_AT + "]-(person)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(personCity:"
                    + Domain.Node.PLACE + ":" + Domain.Place.Type.CITY + "),\n"

                    + "(company)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(companyCountry:" + Domain.Node.PLACE + ":"
                    + Domain.Place.Type.COUNTRY + ")\n"

                    + "RETURN person.%s, person.%s, person.%s, person.%s,\n"

                    + " person.%s, person.%s, person.%s, person.%s, person.%s,\n"

                    + " personCity.%s, uni.%s, studyAt.%s, uniCity.%s, company.%s, worksAt.%s\n"

                    + "companyCountry.%s",

            Domain.Person.FIRST_NAME, Domain.Person.LAST_NAME, Domain.Person.BIRTHDAY, Domain.Person.CREATION_DATE,
                    Domain.Person.GENDER, Domain.Person.LANGUAGES, Domain.Person.BROWSER_USED,
                    Domain.Person.LOCATION_IP, Domain.Person.EMAIL_ADDRESSES, Domain.Place.NAME,
                    Domain.Organisation.NAME, Domain.StudiesAt.CLASS_YEAR, Domain.Place.NAME, Domain.Organisation.NAME,
                    Domain.WorksAt.WORK_FROM, Domain.Place.NAME );

            public static final Map<String, Object> buildParams( String firstName )
            {
                Map<String, Object> queryParams = new HashMap<String, Object>();
                queryParams.put( Domain.Person.FIRST_NAME, firstName );
                return queryParams;
            }
        }

        public static class Query3
        {
            /*
            QUERY 3 
            
            Find Friends and Friends of Friends of the user A that have been to the countries X and Y within a specified period.
             
            PARAMETERS:
             
            Person.Id
            CountryX.Name
            CountryY.Name
            startDate - the beginning of the requested period
            Duration - the duration of the requested period
             
            RETURN:
            
            Person.Id
            ct1 - the number of post from the first country
            ct2 - the number of post from the second country
            ct - ct1 + ct2
             */

            public static final String PERSONS_FOR_PARAMS_TEMPLATE = String.format(

            "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(:" + Domain.Node.PLACE
                    + ":" + Domain.Place.Type.CITY + ")-[:" + Domain.Rel.IS_PART_OF + "]->(country:"
                    + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + ")\n"

                    + "WHERE country." + Domain.Place.NAME + "={country_x} OR country." + Domain.Place.NAME
                    + "={country_y} \n"

                    + "RETURN person." + Domain.Person.ID + ", country." + Domain.Place.NAME + "\n"

                    + "LIMIT 50"

            );

            public static final String QUERY_TEMPLATE = String.format(

            "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(f:" + Domain.Node.PERSON
                    + ")\n"

                    + "WHERE person." + Domain.Person.ID + "={person_id} AND NOT f=person\n"

                    + "WITH DISTINCT f AS friend\n"

                    + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postX:" + Domain.Node.POST + ")-[:"
                    + Domain.Rel.IS_LOCATED_IN + "]->(countryX:" + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY
                    + ")\n"

                    + "WHERE countryX." + Domain.Place.NAME + "={country_x} AND postX." + Domain.Post.CREATION_DATE
                    + ">={min_date} AND postX." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                    + "WITH friend, count(DISTINCT postX) AS xCount\n"

                    + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postY:" + Domain.Node.POST + ")-[:"
                    + Domain.Rel.IS_LOCATED_IN + "]->(countryY:" + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY
                    + ")\n"

                    + "WHERE countryY." + Domain.Place.NAME + "={country_y} AND postY." + Domain.Post.CREATION_DATE
                    + ">={min_date} AND postY." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                    + "WITH friend." + Domain.Person.FIRST_NAME + " +' '+ friend." + Domain.Person.LAST_NAME
                    + " AS friendsName , xCount, count(DISTINCT postY) AS yCount\n"

                    + "RETURN friendsName, xCount, yCount, xCount + yCount AS xyCount"

            );

            public static final Map<String, Object> buildParams( long personId, String countryX, String countryY,
                    Date startDate, int durationDays )
            {
                long minDateInMilli = startDate.getTime();
                Calendar c = Calendar.getInstance();
                c.setTime( startDate );
                c.add( Calendar.DATE, durationDays );
                long maxDateInMilli = c.getTimeInMillis();
                Map<String, Object> queryParams = new HashMap<String, Object>();
                queryParams.put( "person_id", personId );
                queryParams.put( "country_x", countryX );
                queryParams.put( "country_y", countryY );
                queryParams.put( "min_date", minDateInMilli );
                queryParams.put( "max_date", maxDateInMilli );
                return queryParams;
            }
        }

        public static class Query4
        {
            /*
            QUERY 4
            
            TODO poorly specified
            TODO what is time interval 1/2?
            TODO Why is this even necessary?
            "The query finds tags that are discussed among ones friends in time interval 1 and not in time interval 2.
            Typically the first interval is a short span and interval 2 is the time from the start of the dataset to the start if the first interval. 
            The query will quite often come out empty, however this depends on the size of the intervals."

            find posts by friends
            find comments on those posts
            filter those posts by date (within the 24hours preceding the input parameter date)
            get tags for those comments
            return those tags and their counts
            
            Find top 10 most popular topics-hashtags (by the number of comments and posts) that your friends have been talking 
            about in last 24 hours (parameter), but not before that.        
            
            PARAMETERS:
                    
            Person.Id
            startDate
            Duration
            
            RETURN:
            
            Tag.name
            count
             */

            public static final String QUERY_TEMPLATE = String.format(

            "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "]-(friend:" + Domain.Node.PERSON
                    + ")<-[:" + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.POST + ")-[" + Domain.Rel.HAS_TAG
                    + "]->(tag:" + Domain.Node.TAG + ")\n"

                    + "WHERE person." + Domain.Person.ID + "={person_id} AND post." + Domain.Post.CREATION_DATE
                    + ">={min_date} AND post." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                    + "WITH DISTINCT tag, collect(tag) AS tags\n"

                    + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(tags) AS tagCount\n"

                    + "ORDER BY tagCount DESC\n"

                    + "LIMIT 10"

            );

            public static final Map<String, Object> buildParams( long personId, Date startDate, int durationDays )
            {
                Calendar c = Calendar.getInstance();
                c.setTime( startDate );
                c.add( Calendar.DATE, -durationDays );
                long minDateInMilli = c.getTimeInMillis();
                long maxDateInMilli = startDate.getTime();

                Map<String, Object> queryParams = new HashMap<String, Object>();
                queryParams.put( "person_id", personId );
                queryParams.put( "min_date", minDateInMilli );
                queryParams.put( "max_date", maxDateInMilli );
                return queryParams;
            }
        }
    }
}
