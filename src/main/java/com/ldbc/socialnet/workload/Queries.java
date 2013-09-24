package com.ldbc.socialnet.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;

// TODO generator should mention cardinalities

// TODO different relationship types for different relationships (cypher may be faster)

/*
    TODO cypher questions
    
     (1) when is it recommended to use Labels, and when not?
         does it make sense to try to constrain the query as much as possible, 
         or can the cost of additional label check be noticable?
         
     (2) does awaitIndexesOnline take exactly "duration" to return or <=duration, e.g. if indices come online sooner?
         It would be nice to have a blocking one with no "duration" parameter, 
         i.e. "take as long as you need, but don't return before indices are online"
         
     (3) is there any way to create schema indexes during import, rather than after?         
 */

public class Queries
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

        // Length = 607
        public static final String[] FIRST_NAMES = new String[] { "Cyril", "Mizengo", "Jaime", "Gordon", "Fernando",
                "Richard", "Ahsan", "Abdul Rahman", "Manisha", "Mehran", "Marko", "Rahul", "Li", "Baruch", "Alan",
                "Adam", "K.", "Abdul-Malik", "Lei", "Alexey", "Ernest B", "Emperor of Brazil", "Ai", "Boran",
                "Ebrahim", "Christopher", "Mohammad", "Joao", "Hoang Yen", "Bouchaib", "Juliana", "Aleksander", "V.",
                "Maryna", "Jim", "Ge", "Adisorn", "Fei", "Mahmoud", "Wolfgang", "Daniela", "Louis", "Kazuo", "Agapito",
                "Marc", "Andres", "Xavier", "Sayed", "Ning", "Leila", "Jacqueline", "Marwan", "Andrea", "Christian",
                "Pedro", "Pol", "Howard", "Rachel", "Nipapat", "Igor", "Karl", "Asim", "Donald", "Ding", "Ida", "Chau",
                "Aa Ngurah", "Yacine", "Manfred", "M", "Bing", "Eugen", "Chutima", "Krzysztof", "Carl", "Ashok",
                "Angus", "A. K.", "Elli Robert Fitoussi", "Cornelis", "Daria", "Wladyslaw", "Vitor", "Shweta",
                "Arquimedez", "Dieu Hoa", "Jun", "Cam", "Beatrice", "Bich Phuong", "George", "Amaury Gutiérrez",
                "Hassan", "Isabel", "Shlomo", "Carlo", "Ernst", "Somchai", "A. E.", "Jae-Jin", "Maurice", "Sinn",
                "Silvia", "Mostafa", "Abbas", "Hugh", "Manuel", "Babar", "Priyanka", "Đinh Diễm Liên", "Siad", "Bob",
                "Akmal", "Adriana", "Osama", "Cecilio", "Anthony", "Isao", "Dongshan", "Mehmet", "Luigi", "Ana",
                "Gregorio", "Frederick", "Alexander G.", "Antonio", "Barry", "Jessica", "Balbhadra", "Hernaldo",
                "Bernhard", "Seyni", "Zhi", "Abraham (Bram)", "Mohamed", "Koji", "Abraham",
                "Jharana Bajracharya Rashid", "Henry", "Bill", "Ashin", "Shinji", "Álvaro", "Piers", "Carolina",
                "Yang", "Marin", "Baby", "Ali", "Bichang", "Fernand", "Mikhail", "Chokri", "Mustafa", "Alexei",
                "Alejandro", "Werner", "Sofiane", "Anatoliy", "Elena", "Huong", "Sebastian", "Chris", "Wojciech",
                "Matias", "Daisuke", "Bin", "Angel", "Eddie", "Gheorghe", "Ricardo", "Azat", "Hiroshi", "Kirill",
                "Benedictus", "Faiz", "Wei", "Mike", "Michael Andrew", "Seung-Hoon", "Alexandre", "Julien", "Imtiaz",
                "Michel", "Anwar Mohamed", "Baoping", "Kwokhing", "Blanca", "Kenji", "Albade", "Ajuma", "Kalu",
                "Youssef", "Emil", "Juan", "Abby", "Angela", "Sandor", "Walter", "Roberto", "Georges", "Dimitri",
                "Christoforos", "Chan", "Colin", "Asher", "Kunal", "Alexandre Song", "Alex Obanda",
                "Yahya Ould Ahmed El", "Chengdong", "Stephen", "Édouard", "Helmut", "Catalin", "Maciej", "Muhammad",
                "Anant", "Carlos", "Jerry", "Abdul Hamid Al", "Peter", "Luciano", "Aleksej", "Zhang", "Maxim",
                "Shmaryahu", "Esti", "Dario", "Tom", "Benhalima", "William", "Hossein", "Bingbing", "Amber", "Ina",
                "Aziza", "Peng", "Jack", "Alam", "Dimitar", "Rajiv", "Teresa", "Abdel", "Jibriel Denewade Omonga",
                "Aamir", "Erol", "Joe", "Abdel-Fadil", "Dmitry", "Dania", "Bobby", "Francis", "Abdel Halim", "Eliyahu",
                "France", "Abdul Baseer", "Afran", "Francisco", "Bruno", "Abdulwahab", "Denis", "Fawad Ramez",
                "Hristos", "Hector", "Chong", "Heitor", "Henri", "Abdul", "Volodymyr", "Aafia", "Sergio", "Amir",
                "Nigina", "Alfredo", "Ivan", "Adrian", "Karan", "Eduard", "Bo-seok", "Ahmad", "Adriaan", "Ashley",
                "Jacques", "Joseph", "Mark", "Ajahn", "Cathy", "Kamel", "Edward", "Dian", "Javier", "Cunxin", "Boaz",
                "Boris", "Wouter", "R.", "Thomas Ilenda", "Atik", "Wilson", "Natalia", "Ernesto", "Tim", "Jerzy",
                "Cleopa", "Alexis", "Batong", "Paul", "Giovanni", "Ching", "Andrei", "Zhong", "Deepak", "Toshio",
                "Guy", "Maria", "Raymond", "Alfred", "Helio", "Hiroyuki", "Chakrit", "Min-Jung", "Avi", "Ronald",
                "Sirak", "Ariel", "Galina", "Diego", "Johan", "Basil", "Lata", "Aldo", "Fatih", "Luiz", "Prakash",
                "Amon", "Agustiar", "Jan-Willem Breure", "Brian", "Faisal", "Frank", "Georg", "Leroy Halirou",
                "Arturo", "Jan", "Adil", "Anucha", "Abdul Haris", "Oleksandr", "Jambyn", "Abdoulatifou", "Alfonso",
                "Heinrich", "Andriy", "Alexander", "Matthew", "Thomas", "Anand", "Giuseppe", "Piotr", "Chito", "Ahmet",
                "Aisso", "Dmytro", "Fritz", "Fali Sam", "Ahmed Mubarak Obaid", "Ana Paula", "Daniel", "Eugenia",
                "Francesco", "Timo", "Narendra Babubhai", "Martynas", "Abida", "Paolo", "Palghat", "Josue", "Abdallah",
                "Alexandra", "Atef", "Dan", "Adem", "Bingjian", "Benedict", "Takashi", "Arun", "Takeshi", "Patricia",
                "Keith", "Angelo", "Franz", "Aburizal", "Zbigniew", "Eric", "Arjun", "Ah-Joong", "Hideki",
                "Princess Alia", "Ganesh", "Guillermo", "David", "Aleksandr", "Bo", "Matt", "Eleni", "Albaye Papa",
                "Leonardo", "C. S.", "Lakshmi", "Eun-Hye", "Bingyi", "Dmitri", "Jie", "Anupam", "Cesar", "Mahinda",
                "Salma", "Meera", "Babu Sherpa", "Anang", "Constantin", "Pascal", "Fernanda", "Jordi", "Dominique",
                "Charles", "Erich", "Amrozi Bin", "Steve", "Robert", "Arif", "Jeremy", "Ken", "Sarath", "Abhishek",
                "Mordechai", "Lev", "Alfred Sorongo", "Eid", "Michael", "Marcio Ivanildo Da", "Binh", "Michal",
                "Shigeru", "Nelson", "A.", "Atsushi", "Mohammad Ali", "Frija", "Amit", "Crown Prince", "Mary", "André",
                "Javed", "Zeki", "Jean", "Aama", "Poul", "Samuel", "Mamoon Eshaq 0", "Marcio", "Barbara Kanam",
                "Cheng", "Dominic", "Stanisław", "Zakhele", "Dame", "Rudolf", "Gunadasa", "Hans", "Anh", "Amin",
                "Luis", "Zafer", "Taufik", "Asad Amanat Ali", "Scott", "Michelle", "Jimmy", "Andrew", "Chunlai",
                "Rafael", "Ole", "Dawa Dolma", "Roberts", "Nikhil", "Evelyn", "Andre", "Changpeng", "Ouwo Moussa",
                "Marcelo", "Georgi", "Mazor", "Yuki", "Vasil", "Leandro", "Alexandru", "Deepa", "Ahmed", "Jetsada",
                "Seung-Won", "Zheng", "Gabriela", "Chico", "Bulent", "Imran", "Kittipol", "Dichen", "Alberto", "Jose",
                "Jumma", "John", "Vichara", "Mario", "Akbar", "Hao", "Banharn", "Talat", "René", "Artika Sari",
                "Sinan", "Eugenio", "Harry", "Pierre", "Brigitte", "Caroline", "Tariq", "Tibor", "Kamal",
                "Ajuma Nasenyana", "Adolfo", "Katarina", "Raj Ballav", "Hamid", "Sam", "Abdul Wahid", "James",
                "Billinjer C", "Preap", "Yitzhak", "Ian", "Chuan", "Choi", "Mwamba", "Vinod", "Joaquim", "Chen",
                "Corneilius Tiburcio", "Kaoru", "Mauricio", "Esti Mamo", "Jorge", "Aditya", "Anerood",
                "Nassour Guelendouksia", "Antero", "Ibrahim", "Anson", "Laura", "Lin", "Yaniv", "Annemarie",
                "Nicholas", "Friedrich", "Pablo", "Ann", "Chun", "Fekri Al", "Bit Na", "Bishop Augustin", "Rene",
                "Fung", "Anna", "Anıl", "Karim", "Albert", "Marcin", "Avraham", "Tep", "Leonid", "Anuar", "Josef",
                "Burak", "Reza", "Eduardo", "Cy", "Sanjay", "Daniil", "Mathieu", "Gustaaf van", "Naresh", "Akira",
                "Gabriel", "Abu Hamza", "Adel", "Emilio", "Ayesha", "Hichem", "Arthur", "Zaenal", "Nicolae", "Otto",
                "Ahmad Rafiq", "Benny", "Allahshukur", "Adan Mohamed Nuur" };

        public static final String QUERY_TEMPLATE = String.format(

        "MATCH (person:" + Domain.Node.PERSON + ")\n"

        + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.FIRST_NAME + ")\n"

        + "WHERE person." + Domain.Person.FIRST_NAME + "={ person_first_name }\n"

        + "WITH person\n"

        + "ORDER BY person." + Domain.Person.LAST_NAME + "\n"

        + "LIMIT {limit}\n"

        + "MATCH (person)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(personCity:" + Domain.Node.PLACE + ":"
                + Domain.Place.Type.CITY + ")\n"

                + "WITH person, personCity\n"

                + "MATCH (uniCity:" + Domain.Place.Type.CITY + ")<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(uni:"
                + Domain.Organisation.Type.UNIVERSITY + ")<-[studyAt:" + Domain.Rel.STUDY_AT + "]-(person)\n"

                + "WITH collect(DISTINCT (uni." + Domain.Organisation.NAME + " + ', ' + uniCity." + Domain.Place.NAME
                + "+ '(' + studyAt." + Domain.StudiesAt.CLASS_YEAR + " + ')')) AS unis,\n"

                + "  person, personCity\n"

                + "MATCH (companyCountry:" + Domain.Node.PLACE + ":" + Domain.Place.Type.COUNTRY + ")<-[:"
                + Domain.Rel.IS_LOCATED_IN + "]-(company:" + Domain.Organisation.Type.COMPANY + ")<-[worksAt:"
                + Domain.Rel.WORKS_AT + "]-(person)\n"

                + "WITH collect(DISTINCT (company." + Domain.Organisation.NAME + " + ', ' + companyCountry."
                + Domain.Place.NAME + " + '('+ worksAt." + Domain.WorksAt.WORK_FROM + " + ')')) AS companies,\n"

                + "  unis, person, personCity\n"

                + "RETURN person.%s AS firstName, person.%s AS lastName, person.%s AS birthday,\n"

                + "  person.%s AS creation, person.%s AS gender, person.%s AS languages,\n"

                + "  person.%s AS browser, person.%s AS ip, person.%s AS emails,\n"

                + "  personCity.%s AS personCity, unis, companies",

        Domain.Person.FIRST_NAME, Domain.Person.LAST_NAME, Domain.Person.BIRTHDAY, Domain.Person.CREATION_DATE,
                Domain.Person.GENDER, Domain.Person.LANGUAGES, Domain.Person.BROWSER_USED, Domain.Person.LOCATION_IP,
                Domain.Person.EMAIL_ADDRESSES, Domain.Place.NAME );

        public static final Map<String, Object> buildParams( String firstName, int limit )
        {
            Map<String, Object> queryParams = new HashMap<String, Object>();
            queryParams.put( "person_first_name", firstName );
            queryParams.put( "limit", limit );
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
        startDate - the beginning of the requested period (the latest date)
        Duration - the duration of the requested period
         
        RETURN:
        
        Person.Id
        ct1 = the number of post from the first country
        ct2 = the number of post from the second country
        ct = ct1 + ct2
         */

        public static final String PERSONS_FOR_PARAMS_TEMPLATE = String.format(

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(:" + Domain.Node.PLACE + ":"
                + Domain.Place.Type.CITY + ")-[:" + Domain.Rel.IS_PART_OF + "]->(country:" + Domain.Node.PLACE + ":"
                + Domain.Place.Type.COUNTRY + ")\n"

                + "WHERE country." + Domain.Place.NAME + "={country_x} OR country." + Domain.Place.NAME
                + "={country_y}\n"

                + "RETURN person." + Domain.Person.ID + ", country." + Domain.Place.NAME + "\n"

                + "LIMIT 50"

        );

        public static final String QUERY_TEMPLATE = String.format(

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(f:" + Domain.Node.PERSON + ")\n"

        + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

        + "WHERE person." + Domain.Person.ID + "={person_id}\n"

        + "WITH DISTINCT f AS friend\n"

        + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postX:" + Domain.Node.POST + ")-[:"
                + Domain.Rel.IS_LOCATED_IN + "]->(countryX:" + Domain.Place.Type.COUNTRY + ")\n"

                + "USING INDEX countryX:" + Domain.Place.Type.COUNTRY + "(" + Domain.Place.NAME + ")\n"

                + "WHERE countryX." + Domain.Place.NAME + "={country_x} AND postX." + Domain.Post.CREATION_DATE
                + ">={min_date} AND postX." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                + "WITH friend, count(DISTINCT postX) AS xCount\n"

                + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(postY:" + Domain.Node.POST + ")-[:"
                + Domain.Rel.IS_LOCATED_IN + "]->(countryY:" + Domain.Place.Type.COUNTRY + ")\n"

                + "USING INDEX countryY:" + Domain.Place.Type.COUNTRY + "(" + Domain.Place.NAME + ")\n"

                + "WHERE countryY." + Domain.Place.NAME + "={country_y} AND postY." + Domain.Post.CREATION_DATE
                + ">={min_date} AND postY." + Domain.Post.CREATION_DATE + "<={max_date}\n"

                + "WITH friend." + Domain.Person.FIRST_NAME + " + ' ' + friend." + Domain.Person.LAST_NAME
                + "  AS friendName , xCount, count(DISTINCT postY) AS yCount\n"

                + "RETURN friendName, xCount, yCount, xCount + yCount AS xyCount\n"

                + "ORDER BY xyCount DESC"

        );

        public static final Map<String, Object> buildParams( long personId, String countryX, String countryY,
                Date endDate, int durationDays )
        {
            long maxDateInMilli = endDate.getTime();
            Calendar c = Calendar.getInstance();
            c.setTime( endDate );
            c.add( Calendar.DATE, -durationDays );
            long minDateInMilli = c.getTimeInMillis();
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

        // public static final String QUERY_TEMPLATE = String.format(
        //
        // "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS +
        // "]-(friend:" + Domain.Node.PERSON + ")<-[:"
        // + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.POST + ")-[" +
        // Domain.Rel.HAS_TAG + "]->(tag:"
        // + Domain.Node.TAG + ")\n"
        //
        // + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID
        // + ")\n"
        //
        // + "WHERE person." + Domain.Person.ID + "={person_id} AND post." +
        // Domain.Post.CREATION_DATE
        // + ">={min_date} AND post." + Domain.Post.CREATION_DATE +
        // "<={max_date}\n"
        //
        // + "WITH DISTINCT tag, collect(tag) AS tags\n"
        //
        // + "RETURN tag." + Domain.Tag.NAME +
        // " AS tagName, length(tags) AS tagCount\n"
        //
        // + "ORDER BY tagCount DESC\n"
        //
        // + "LIMIT 10"
        //
        // );

        public static final String QUERY_TEMPLATE = String.format(

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "]-(friend:" + Domain.Node.PERSON + ")\n"

        + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

        + "WHERE person." + Domain.Person.ID + "={person_id}\n"

        + "WITH friend\n"

        + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.POST + ")\n"

        + "WHERE post." + Domain.Post.CREATION_DATE + ">={min_date} AND post." + Domain.Post.CREATION_DATE
                + "<={max_date}\n"

                + "WITH post\n"

                + "MATCH (post)-[" + Domain.Rel.HAS_TAG + "]->(tag:" + Domain.Node.TAG + ")\n"

                + "WITH DISTINCT tag, collect(tag) AS tags\n"

                + "RETURN tag." + Domain.Tag.NAME + " AS tagName, length(tags) AS tagCount\n"

                + "ORDER BY tagCount DESC\n"

                + "LIMIT 10"

        );

        public static final Map<String, Object> buildParams( long personId, Date endDate, int durationDays )
        {
            Calendar c = Calendar.getInstance();
            c.setTime( endDate );
            c.add( Calendar.DATE, -durationDays );
            long minDateInMilli = c.getTimeInMillis();
            long maxDateInMilli = endDate.getTime();

            Map<String, Object> queryParams = new HashMap<String, Object>();
            queryParams.put( "person_id", personId );
            queryParams.put( "min_date", minDateInMilli );
            queryParams.put( "max_date", maxDateInMilli );
            return queryParams;
        }
    }

    public static class Query5
    {
        /*
        QUERY 5

        Description
            What are the groups that your connections (friendship up to second hop) have joined after a certain date? 
            Order them by the number of posts and comments your connections made there.
        
        PARAMETERS:                
            Person
            Date
        
        RETURN:
            Group
            count
         */

        /*
            cakesAndPies - 2013, Calendar.OCTOBER, 2
                Alex - 2013, Calendar.OCTOBER, 2 
                Aiya - 2013, Calendar.OCTOBER, 3
                Stranger - 2013, Calendar.OCTOBER, 4
                Jake - 2013, Calendar.OCTOBER, 8 

            redditAddicts - 2013, Calendar.OCTOBER, 22
                Jake - 2013, Calendar.OCTOBER, 22

            floatingBoats - 2013, Calendar.NOVEMBER, 13
                Jake - 2013, Calendar.NOVEMBER, 13 
                Alex - 2013, Calendar.NOVEMBER, 14
                Peter -  2013, Calendar.NOVEMBER, 16 

            kiwisSheepAndBungyJumping - 2013, Calendar.NOVEMBER, 1
                Aiya - 2013, Calendar.NOVEMBER, 1
                Alex - 2013, Calendar.NOVEMBER, 4
         */

        public static final String QUERY_TEMPLATE_posts =

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(friend:" + Domain.Node.PERSON
                + ")\n"

                + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

                + "WHERE person." + Domain.Person.ID + "={person_id}\n"

                + "WITH friend\n"

                + "MATCH (friend)<-[membership:" + Domain.Rel.HAS_MEMBER + "]-(forum:" + Domain.Node.FORUM + ")\n"

                + "WHERE membership." + Domain.HasMember.JOIN_DATE + ">{join_date}\n"

                + "WITH forum, friend\n"

                + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.POST + ")<-[:"
                + Domain.Rel.CONTAINER_OF + "]-(forum)\n"

                + "RETURN forum.title AS forum, count(post) AS posts\n"

                + "ORDER BY posts DESC"

        ;

        public static final String QUERY_TEMPLATE_comments =

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(friend:" + Domain.Node.PERSON
                + ")\n"

                + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

                + "WHERE person." + Domain.Person.ID + "={person_id}\n"

                + "WITH friend\n"

                + "MATCH (friend)<-[membership:" + Domain.Rel.HAS_MEMBER + "]-(forum:" + Domain.Node.FORUM + ")\n"

                + "WHERE membership." + Domain.HasMember.JOIN_DATE + ">{join_date}\n"

                + "WITH forum, friend\n"

                + "MATCH (friend)<-[:" + Domain.Rel.HAS_CREATOR + "]-(comment:" + Domain.Node.COMMENT + ")\n"

                + "WHERE (comment)-[:" + Domain.Rel.REPLY_OF + "*0..]->(:" + Domain.Node.COMMENT + ")-[:"
                + Domain.Rel.REPLY_OF + "]->(:" + Domain.Node.POST + ")<-[:" + Domain.Rel.CONTAINER_OF + "]-(forum)\n"

                + "RETURN forum.title AS forum, count(comment) AS comments\n"

                + "ORDER BY comments DESC"

        ;

        public static final Map<String, Object> buildParams( long personId, Date date )
        {
            Map<String, Object> queryParams = new HashMap<String, Object>();
            queryParams.put( "person_id", personId );
            queryParams.put( "join_date", date.getTime() );
            return queryParams;
        }
    }

    public static class Query6
    {
        /*
        QUERY 6

        STORY 
        
            People who discuss X also discuss.
            Find 10 most popular Tags of people that are connected to you via friendship path and talk about topic/Tag 'X'.
        
        DESCRIPTION

            Among POSTS by FRIENDS and FRIENDS OF FRIENDS, find the TAGS most commonly occurring together with a given TAG.

        PARAMETERS
        
            Person
            Source.tag
                        
        RETURN
        
            Tag.name
            count
         */

        /*
         */

        public static final String QUERY_TEMPLATE =

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.KNOWS + "*1..2]-(:" + Domain.Node.PERSON + ")<-[:"
                + Domain.Rel.HAS_CREATOR + "]-(post:" + Domain.Node.POST + ")-[:" + Domain.Rel.HAS_TAG + "]->(tag:"
                + Domain.Node.TAG + ")\n"

                + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

                + "USING INDEX tag:" + Domain.Node.TAG + "(" + Domain.Tag.NAME + ")\n"

                + "WHERE person." + Domain.Person.ID + "={person_id} AND tag." + Domain.Tag.NAME + "={tag_name}\n"

                + "WITH DISTINCT post\n"

                + "MATCH (post)-[:" + Domain.Rel.HAS_TAG + "]->(tag:" + Domain.Node.TAG + ")\n"

                + "WHERE NOT(tag." + Domain.Tag.NAME + "={tag_name})\n"

                + "RETURN tag." + Domain.Tag.NAME + " AS tag, count(tag) AS count\n"

                + "ORDER BY count DESC\n"

                + "LIMIT 10"

        ;

        public static final Map<String, Object> buildParams( long personId, String tagName )
        {
            Map<String, Object> queryParams = new HashMap<String, Object>();
            queryParams.put( "person_id", personId );
            queryParams.put( "tag_name", tagName );
            return queryParams;
        }
    }

    public static class Query7
    {
        /*
        QUERY 7
        
        DESCRIPTION
            Find the 10 most popular tags that occurred in your country during the last X hours, and that none of your friends has discussed.

        PARAMETERS
            Person
            startDateTime
            duration (hours)
                        
        RETURN
            Tag.name
            count        
         */

        public static final String QUERY_TEMPLATE =

        "MATCH (person:" + Domain.Node.PERSON + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(:" + Domain.Place.Type.CITY
                + ")-[:" + Domain.Rel.IS_LOCATED_IN + "]->(country:" + Domain.Place.Type.COUNTRY + ")\n"

                + "USING INDEX person:" + Domain.Node.PERSON + "(" + Domain.Person.ID + ")\n"

                + "WHERE person." + Domain.Person.ID + "={person_id}\n"

                + "WITH person, country\n"

                + "MATCH (person)-[:" + Domain.Rel.KNOWS + "]->(friend:" + Domain.Node.PERSON + ")\n"

                + "WITH friend, country\n"

                + "MATCH (country)<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(post:" + Domain.Node.POST + ")-[:"
                + Domain.Rel.HAS_TAG + "]->(tag:" + Domain.Node.TAG + ")\n"

                // + "WHERE NOT((tag)<-[:" + Domain.Rel.HAS_TAG + "]-(:" +
                // Domain.Node.POST + ")-[:"
                // + Domain.Rel.HAS_CREATOR + "]->(friend))\n"

                + "RETURN tag.name AS tag, post.content AS post, country.name AS country\n"

        // + "RETURN DISTINCT tag.name AS tag, count(tag) AS count\n"
        //
        // + "ORDER BY count DESC\n"
        //
        // + "LIMIT 10"

        ;

        public static final Map<String, Object> buildParams( long personId, Date endDateTime, int durationHours )
        {
            long maxDateInMilli = endDateTime.getTime();
            Calendar c = Calendar.getInstance();
            c.setTime( endDateTime );
            c.add( Calendar.HOUR, -durationHours );
            long minDateInMilli = c.getTimeInMillis();

            Map<String, Object> queryParams = new HashMap<String, Object>();
            queryParams.put( "person_id", personId );
            queryParams.put( "min_date", minDateInMilli );
            queryParams.put( "max_date", maxDateInMilli );
            return queryParams;
        }
    }

    /*
    _Find the 10 most popular topics/tags that occurred in your country during the last X hours, and that none of your friends has discussed_
    _Find all tags in a short time window and list the ones that are not discussed by your friends_
     */
}
