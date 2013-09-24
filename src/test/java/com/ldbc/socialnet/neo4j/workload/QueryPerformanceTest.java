package com.ldbc.socialnet.neo4j.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.socialnet.workload.Queries;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class QueryPerformanceTest
{
    public static final boolean PRINT = true;

    public static GraphDatabaseService db = null;
    public static ExecutionEngine queryEngine = null;

    @BeforeClass
    public static void openDb()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( Config.DB_DIR ).setConfig( Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        queryEngine = new ExecutionEngine( db );
    }

    @AfterClass
    public static void closeDb()
    {
        db.shutdown();
    }

    @Test
    public void names()
    {
        // GraphDatabaseService db = new
        // GraphDatabaseFactory().newEmbeddedDatabaseBuilder( Config.DB_DIR
        // ).setConfig(
        // Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        //
        // ExecutionEngine queryEngine = new ExecutionEngine( db );

        String query =

        "MATCH (person:PERSON)\n"

        + "RETURN DISTINCT person.firstName";

        ExecutionResult result = queryEngine.execute( query );
        System.out.println( result.dumpToString() );

        String[] firstNames = new String[] { "Cyril", "Mizengo", "Jaime", "Gordon", "Fernando", "Richard", "Ahsan",
                "Abdul Rahman", "Manisha", "Mehran", "Marko", "Rahul", "Li", "Baruch", "Alan", "Adam", "K.",
                "Abdul-Malik", "Lei", "Alexey", "Ernest B", "Emperor of Brazil", "Ai", "Boran", "Ebrahim",
                "Christopher", "Mohammad", "Joao", "Hoang Yen", "Bouchaib", "Juliana", "Aleksander", "V.", "Maryna",
                "Jim", "Ge", "Adisorn", "Fei", "Mahmoud", "Wolfgang", "Daniela", "Louis", "Kazuo", "Agapito", "Marc",
                "Andres", "Xavier", "Sayed", "Ning", "Leila", "Jacqueline", "Marwan", "Andrea", "Christian", "Pedro",
                "Pol", "Howard", "Rachel", "Nipapat", "Igor", "Karl", "Asim", "Donald", "Ding", "Ida", "Chau",
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
    }

    @Test
    public void query1()
    {
        // Map<String, Object> queryParams = Queries.Query1.buildParams( "Chen",
        // 10 );
        Map<String, Object> queryParams = Queries.Query1.buildParams( "John", 10 );
        execute( "Query1", Queries.Query1.QUERY_TEMPLATE, queryParams, 5, 5, false );
    }

    @Test
    public void query3()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2010, Calendar.JANUARY, 1 );
        Date startDate = calendar.getTime();
        int durationDays = 365 * 1;

        long personId = 143;
        String countryX = "United_States";
        String countryY = "Canada";

        Map<String, Object> queryParams = Queries.Query3.buildParams( personId, countryX, countryY, startDate,
                durationDays );
        execute( "Query3", Queries.Query3.QUERY_TEMPLATE, queryParams, 5, 5, false );

        // personId = 405;
        // countryX = "India";
        // countryY = "Pakistan";
        //
        // queryParams = Queries.Query3.buildParams( personId,
        // countryX, countryY, startDate, durationDays );
        // execute( Queries.Query3.QUERY_TEMPLATE, queryParams,
        // 2, 10, true, false );
    }

    @Test
    public void query4()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );

        long personId = 143;
        Date startDate = calendar.getTime();
        int durationDays = 300;

        Map<String, Object> queryParams = Queries.Query4.buildParams( personId, startDate, durationDays );
        execute( "Query4", Queries.Query4.QUERY_TEMPLATE, queryParams, 5, 5, false );
    }

    @Test
    public void query5()
    {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2011, Calendar.JANUARY, 1 );

        long personId = 143;
        Date joinDate = calendar.getTime();

        Map<String, Object> queryParams = Queries.Query5.buildParams( personId, joinDate );
        execute( "Query5 - posts", Queries.Query5.QUERY_TEMPLATE_posts, queryParams, 5, 5, false );
        execute( "Query5 - comments", Queries.Query5.QUERY_TEMPLATE_comments, queryParams, 5, 5, false );
    }

    @Test
    public void query6()
    {
        long personId = 143;

        String tagName = "Charles_Dickens";

        Map<String, Object> queryParams = Queries.Query6.buildParams( personId, tagName );

        // queryParams = new HashMap<String, Object>();
        //
        // String prepQuery =
        //
        // "MATCH (person:PERSON)-[:KNOWS*1..2]-(:PERSON)<-[:HAS_CREATOR]-(:POST)-[:HAS_TAG]->(tag:TAG)\n"
        //
        // + "WHERE person.id=143\n"
        //
        // + "RETURN tag.name, count(tag) AS count\n"
        //
        // + "ORDER BY count DESC\n"
        //
        // + "LIMIT 10";

        /*
        +---------------------------------+
         tag.name                 count 
        +---------------------------------+
         "Charles_Dickens"        8036  
         "Heinrich_Himmler"       7524  
         "Herman_Melville"        7092  
         "Bottle_Pop"             6920  
         "Johann_Sebastian_Bach"  6392  
         "Theodore_Roosevelt"     6140  
         "David_Gilmour"          5744  
         "Winston_Churchill"      5516  
         "Martin_Van_Buren"       5380  
         "Galileo_Galilei"        5368  
        +---------------------------------+
         */

        // execute( "Query6 - prep", prepQuery, queryParams, 0, 1, false );
        execute( "Query6", Queries.Query6.QUERY_TEMPLATE, queryParams, 5, 5, false );
    }

    private void execute( String name, String queryString, Map<String, Object> queryParams, long warmup,
            long iterations, boolean profile )
    {
        queryString = ( profile ) ? "profile\n" + queryString : queryString;
        if ( PRINT )
        {
            System.out.println( queryParams.toString() );
            System.out.println();
            System.out.println( queryString );
        }
        for ( int i = 0; i < warmup; i++ )
        {
            queryEngine.execute( queryString, queryParams );
            if ( PRINT ) System.out.print( "?" );
        }
        long runtimeTotal = 0;
        long runtimeRuns = 0;
        long runtimeMin = Long.MAX_VALUE;
        long runtimeMax = Long.MIN_VALUE;
        ExecutionResult result = null;
        for ( int i = 0; i < iterations; i++ )
        {
            long start = System.currentTimeMillis();
            result = queryEngine.execute( queryString, queryParams );
            long runtime = System.currentTimeMillis() - start;
            runtimeTotal += runtime;
            runtimeRuns++;
            runtimeMin = Math.min( runtimeMin, runtime );
            runtimeMax = Math.max( runtimeMax, runtime );
            if ( PRINT ) System.out.print( "!" );
        }
        if ( PRINT )
        {
            System.out.println();
            System.out.println( "\truntimeTotal=" + runtimeTotal );
            System.out.println( "\truntimeRuns=" + runtimeRuns );
            System.out.println( "\truntimeMin=" + runtimeMin );
            System.out.println( "\truntimeMax=" + runtimeMax );
            System.out.println( "\truntimeMean=" + runtimeTotal / runtimeRuns );
            System.out.println( result.dumpToString() );
        }
        else
        {
            System.out.println( String.format( "%s: runtime mean=%s(ms)", name, runtimeTotal / runtimeRuns ) );
        }
    }
}
