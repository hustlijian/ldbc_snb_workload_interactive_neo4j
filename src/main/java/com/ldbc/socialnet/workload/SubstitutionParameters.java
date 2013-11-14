package com.ldbc.socialnet.workload;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.traversal.steps.execution.StepsUtils;

import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.ldbc.socialnet.workload.neo4j.utils.Config;

import static com.ldbc.socialnet.workload.Domain.*;

public class SubstitutionParameters
{
    // Length = 607
    public static final String[] FIRST_NAMES = new String[] { "Cyril", "Mizengo", "Jaime", "Gordon", "Fernando",
            "Richard", "Ahsan", "Abdul Rahman", "Manisha", "Mehran", "Marko", "Rahul", "Li", "Baruch", "Alan", "Adam",
            "K.", "Abdul-Malik", "Lei", "Alexey", "Ernest B", "Emperor of Brazil", "Ai", "Boran", "Ebrahim",
            "Christopher", "Mohammad", "Joao", "Hoang Yen", "Bouchaib", "Juliana", "Aleksander", "V.", "Maryna", "Jim",
            "Ge", "Adisorn", "Fei", "Mahmoud", "Wolfgang", "Daniela", "Louis", "Kazuo", "Agapito", "Marc", "Andres",
            "Xavier", "Sayed", "Ning", "Leila", "Jacqueline", "Marwan", "Andrea", "Christian", "Pedro", "Pol",
            "Howard", "Rachel", "Nipapat", "Igor", "Karl", "Asim", "Donald", "Ding", "Ida", "Chau", "Aa Ngurah",
            "Yacine", "Manfred", "M", "Bing", "Eugen", "Chutima", "Krzysztof", "Carl", "Ashok", "Angus", "A. K.",
            "Elli Robert Fitoussi", "Cornelis", "Daria", "Wladyslaw", "Vitor", "Shweta", "Arquimedez", "Dieu Hoa",
            "Jun", "Cam", "Beatrice", "Bich Phuong", "George", "Amaury Gutiérrez", "Hassan", "Isabel", "Shlomo",
            "Carlo", "Ernst", "Somchai", "A. E.", "Jae-Jin", "Maurice", "Sinn", "Silvia", "Mostafa", "Abbas", "Hugh",
            "Manuel", "Babar", "Priyanka", "Đinh Diễm Liên", "Siad", "Bob", "Akmal", "Adriana", "Osama", "Cecilio",
            "Anthony", "Isao", "Dongshan", "Mehmet", "Luigi", "Ana", "Gregorio", "Frederick", "Alexander G.",
            "Antonio", "Barry", "Jessica", "Balbhadra", "Hernaldo", "Bernhard", "Seyni", "Zhi", "Abraham (Bram)",
            "Mohamed", "Koji", "Abraham", "Jharana Bajracharya Rashid", "Henry", "Bill", "Ashin", "Shinji", "Álvaro",
            "Piers", "Carolina", "Yang", "Marin", "Baby", "Ali", "Bichang", "Fernand", "Mikhail", "Chokri", "Mustafa",
            "Alexei", "Alejandro", "Werner", "Sofiane", "Anatoliy", "Elena", "Huong", "Sebastian", "Chris", "Wojciech",
            "Matias", "Daisuke", "Bin", "Angel", "Eddie", "Gheorghe", "Ricardo", "Azat", "Hiroshi", "Kirill",
            "Benedictus", "Faiz", "Wei", "Mike", "Michael Andrew", "Seung-Hoon", "Alexandre", "Julien", "Imtiaz",
            "Michel", "Anwar Mohamed", "Baoping", "Kwokhing", "Blanca", "Kenji", "Albade", "Ajuma", "Kalu", "Youssef",
            "Emil", "Juan", "Abby", "Angela", "Sandor", "Walter", "Roberto", "Georges", "Dimitri", "Christoforos",
            "Chan", "Colin", "Asher", "Kunal", "Alexandre Song", "Alex Obanda", "Yahya Ould Ahmed El", "Chengdong",
            "Stephen", "Édouard", "Helmut", "Catalin", "Maciej", "Muhammad", "Anant", "Carlos", "Jerry",
            "Abdul Hamid Al", "Peter", "Luciano", "Aleksej", "Zhang", "Maxim", "Shmaryahu", "Esti", "Dario", "Tom",
            "Benhalima", "William", "Hossein", "Bingbing", "Amber", "Ina", "Aziza", "Peng", "Jack", "Alam", "Dimitar",
            "Rajiv", "Teresa", "Abdel", "Jibriel Denewade Omonga", "Aamir", "Erol", "Joe", "Abdel-Fadil", "Dmitry",
            "Dania", "Bobby", "Francis", "Abdel Halim", "Eliyahu", "France", "Abdul Baseer", "Afran", "Francisco",
            "Bruno", "Abdulwahab", "Denis", "Fawad Ramez", "Hristos", "Hector", "Chong", "Heitor", "Henri", "Abdul",
            "Volodymyr", "Aafia", "Sergio", "Amir", "Nigina", "Alfredo", "Ivan", "Adrian", "Karan", "Eduard",
            "Bo-seok", "Ahmad", "Adriaan", "Ashley", "Jacques", "Joseph", "Mark", "Ajahn", "Cathy", "Kamel", "Edward",
            "Dian", "Javier", "Cunxin", "Boaz", "Boris", "Wouter", "R.", "Thomas Ilenda", "Atik", "Wilson", "Natalia",
            "Ernesto", "Tim", "Jerzy", "Cleopa", "Alexis", "Batong", "Paul", "Giovanni", "Ching", "Andrei", "Zhong",
            "Deepak", "Toshio", "Guy", "Maria", "Raymond", "Alfred", "Helio", "Hiroyuki", "Chakrit", "Min-Jung", "Avi",
            "Ronald", "Sirak", "Ariel", "Galina", "Diego", "Johan", "Basil", "Lata", "Aldo", "Fatih", "Luiz",
            "Prakash", "Amon", "Agustiar", "Jan-Willem Breure", "Brian", "Faisal", "Frank", "Georg", "Leroy Halirou",
            "Arturo", "Jan", "Adil", "Anucha", "Abdul Haris", "Oleksandr", "Jambyn", "Abdoulatifou", "Alfonso",
            "Heinrich", "Andriy", "Alexander", "Matthew", "Thomas", "Anand", "Giuseppe", "Piotr", "Chito", "Ahmet",
            "Aisso", "Dmytro", "Fritz", "Fali Sam", "Ahmed Mubarak Obaid", "Ana Paula", "Daniel", "Eugenia",
            "Francesco", "Timo", "Narendra Babubhai", "Martynas", "Abida", "Paolo", "Palghat", "Josue", "Abdallah",
            "Alexandra", "Atef", "Dan", "Adem", "Bingjian", "Benedict", "Takashi", "Arun", "Takeshi", "Patricia",
            "Keith", "Angelo", "Franz", "Aburizal", "Zbigniew", "Eric", "Arjun", "Ah-Joong", "Hideki", "Princess Alia",
            "Ganesh", "Guillermo", "David", "Aleksandr", "Bo", "Matt", "Eleni", "Albaye Papa", "Leonardo", "C. S.",
            "Lakshmi", "Eun-Hye", "Bingyi", "Dmitri", "Jie", "Anupam", "Cesar", "Mahinda", "Salma", "Meera",
            "Babu Sherpa", "Anang", "Constantin", "Pascal", "Fernanda", "Jordi", "Dominique", "Charles", "Erich",
            "Amrozi Bin", "Steve", "Robert", "Arif", "Jeremy", "Ken", "Sarath", "Abhishek", "Mordechai", "Lev",
            "Alfred Sorongo", "Eid", "Michael", "Marcio Ivanildo Da", "Binh", "Michal", "Shigeru", "Nelson", "A.",
            "Atsushi", "Mohammad Ali", "Frija", "Amit", "Crown Prince", "Mary", "André", "Javed", "Zeki", "Jean",
            "Aama", "Poul", "Samuel", "Mamoon Eshaq 0", "Marcio", "Barbara Kanam", "Cheng", "Dominic", "Stanisław",
            "Zakhele", "Dame", "Rudolf", "Gunadasae", "Hans", "Anh", "Amin", "Luis", "Zafer", "Taufik",
            "Asad Amanat Ali", "Scott", "Michelle", "Jimmy", "Andrew", "Chunlai", "Rafael", "Ole", "Dawa Dolma",
            "Roberts", "Nikhil", "Evelyn", "Andre", "Changpeng", "Ouwo Moussa", "Marcelo", "Georgi", "Mazor", "Yuki",
            "Vasil", "Leandro", "Alexandru", "Deepa", "Ahmed", "Jetsada", "Seung-Won", "Zheng", "Gabriela", "Chico",
            "Bulent", "Imran", "Kittipol", "Dichen", "Alberto", "Jose", "Jumma", "John", "Vichara", "Mario", "Akbar",
            "Hao", "Banharn", "Talat", "René", "Artika Sari", "Sinan", "Eugenio", "Harry", "Pierre", "Brigitte",
            "Caroline", "Tariq", "Tibor", "Kamal", "Ajuma Nasenyana", "Adolfo", "Katarina", "Raj Ballav", "Hamid",
            "Sam", "Abdul Wahid", "James", "Billinjer C", "Preap", "Yitzhak", "Ian", "Chuan", "Choi", "Mwamba",
            "Vinod", "Joaquim", "Chen", "Corneilius Tiburcio", "Kaoru", "Mauricio", "Esti Mamo", "Jorge", "Aditya",
            "Anerood", "Nassour Guelendouksia", "Antero", "Ibrahim", "Anson", "Laura", "Lin", "Yaniv", "Annemarie",
            "Nicholas", "Friedrich", "Pablo", "Ann", "Chun", "Fekri Al", "Bit Na", "Bishop Augustin", "Rene", "Fung",
            "Anna", "Anıl", "Karim", "Albert", "Marcin", "Avraham", "Tep", "Leonid", "Anuar", "Josef", "Burak", "Reza",
            "Eduardo", "Cy", "Sanjay", "Daniil", "Mathieu", "Gustaaf van", "Naresh", "Akira", "Gabriel", "Abu Hamza",
            "Adel", "Emilio", "Ayesha", "Hichem", "Arthur", "Zaenal", "Nicolae", "Otto", "Ahmad Rafiq", "Benny",
            "Allahshukur", "Adan Mohamed Nuur" };

    public static void main( String[] args )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( Config.DB_DIR ).setConfig(
                Config.NEO4J_RUN_CONFIG ).newGraphDatabase();
        ExecutionEngine engine = new ExecutionEngine( db );
        try
        {
            System.out.println( "First names: " + firstNames( engine ).size() );
            System.out.println( "Post creation dates: " + postCreationDates( engine ).size() );
            System.out.println( "Person IDs: " + personIds( engine ).size() );
            System.out.println( "Tag URIs: " + tagUris( engine ).size() );
            System.out.println( "Horoscope signs: " + horoscopeSigns().size() );
            System.out.println( "Country URIs: " + countryUris( engine ).size() );
            System.out.println( "Work-from dates: " + workFromDates( engine ).size() );
            System.out.println( "Tag classes: " + tagClassUris( engine ).size() );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        finally
        {
            db.shutdown();
        }
    }

    public static final Charset DEFAULT_CHARSET = Charset.forName( "UTF-8" );

    /*
    http://www.ldbc.eu:8090/display/TUC/IW+Substitution+parameters+selection
    
    GENERATE
     - personNames.txt
     - personNumber.txt
     - creationPostDate.txt 
         0,33,66,100 percents of date range... or maybe all 0..100
         duration of date range
     - tagUris.txt
     - countryUris.txt (orgLocations.txt)
     - workFromDate.txt
     - tagClassUris.txt 
    PROVIDED 
    - countryPairs.txt
     */

    public static List<String> firstNames( ExecutionEngine engine )
    {
        String query = "MATCH (person:" + Nodes.Person + ")\n"

        + "RETURN person." + Person.FIRST_NAME + " AS name";

        return ImmutableList.copyOf( StepsUtils.distinct( Iterables.transform( engine.execute( query ),
                new Function<Map<String, Object>, String>()
                {
                    @Override
                    public String apply( Map<String, Object> result )
                    {
                        return (String) result.get( "name" );
                    }
                } ) ) );
    }

    public static Map<Integer, Long> postCreationDates( ExecutionEngine engine )
    {
        String query = "MATCH (post:" + Nodes.Post + ")\n"

        + "WITH post." + Post.CREATION_DATE + " AS date\n"

        + "RETURN date\n"

        + "ORDER BY date DESC";

        List<Long> creationDates = ImmutableList.copyOf( Iterables.transform( engine.execute( query ),
                new Function<Map<String, Object>, Long>()
                {
                    @Override
                    public Long apply( Map<String, Object> result )
                    {
                        return (long) result.get( "date" );
                    }
                } ) );

        final long percent100CreationDateAsMilli = creationDates.get( 0 );
        final long percent0CreationDateAsMilli = creationDates.get( creationDates.size() - 1 );
        final long range = percent100CreationDateAsMilli - percent0CreationDateAsMilli;
        final long onePercentOfRange = range / 100;

        Map<Integer, Long> creationDateRangePercentages = new HashMap<Integer, Long>();
        for ( Integer percentage : ContiguousSet.create( Range.closed( 0, 100 ), DiscreteDomain.integers() ) )
        {
            long creationDateRangePercentage = percent0CreationDateAsMilli + ( onePercentOfRange * percentage );
            creationDateRangePercentages.put( percentage, creationDateRangePercentage );
        }
        return creationDateRangePercentages;
    }

    public static List<Long> personIds( ExecutionEngine engine )
    {
        String query = "MATCH (person:" + Nodes.Person + ")\n"

        + "RETURN person." + Person.ID + " AS id";

        return ImmutableList.copyOf( StepsUtils.distinct( Iterables.transform( engine.execute( query ),
                new Function<Map<String, Object>, Long>()
                {
                    @Override
                    public Long apply( Map<String, Object> result )
                    {
                        return (long) result.get( "id" );
                    }
                } ) ) );
    }

    public static List<String> tagUris( ExecutionEngine engine )
    {
        String query = "MATCH (tag:" + Nodes.Tag + ")\n"

        + "RETURN tag." + Tag.URI + " AS uri";

        return ImmutableList.copyOf( StepsUtils.distinct( Iterables.transform( engine.execute( query ),
                new Function<Map<String, Object>, String>()
                {
                    @Override
                    public String apply( Map<String, Object> result )
                    {
                        return (String) result.get( "uri" );
                    }
                } ) ) );
    }

    public static List<Integer> horoscopeSigns()
    {
        return ImmutableList.copyOf( ContiguousSet.create( Range.closed( 1, 12 ), DiscreteDomain.integers() ) );
    }

    public static List<String> countryUris( ExecutionEngine engine )
    {
        String query = "MATCH (country:" + Place.Type.Country + ")\n"

        + "RETURN country." + Place.URI + " AS uri";

        return ImmutableList.copyOf( StepsUtils.distinct( Iterables.transform( engine.execute( query ),
                new Function<Map<String, Object>, String>()
                {
                    @Override
                    public String apply( Map<String, Object> result )
                    {
                        return (String) result.get( "uri" );
                    }
                } ) ) );
    }

    public static List<Integer> workFromDates( ExecutionEngine engine )
    {
        String query = "MATCH ()-[workFrom:" + Rels.WORKS_AT + "]->()\n"

        + "RETURN workFrom." + WorksAt.WORK_FROM + " AS workFrom";

        return ImmutableList.copyOf( StepsUtils.distinct( Iterables.transform( engine.execute( query ),
                new Function<Map<String, Object>, Integer>()
                {
                    @Override
                    public Integer apply( Map<String, Object> result )
                    {
                        return (Integer) result.get( "workFrom" );
                    }
                } ) ) );
    }

    public static List<String> tagClassUris( ExecutionEngine engine )
    {
        String query = "MATCH (tagClass:" + Nodes.TagClass + ")\n"

        + "RETURN tagClass." + TagClass.URI + " AS uri";

        return ImmutableList.copyOf( StepsUtils.distinct( Iterables.transform( engine.execute( query ),
                new Function<Map<String, Object>, String>()
                {
                    @Override
                    public String apply( Map<String, Object> result )
                    {
                        return (String) result.get( "uri" );
                    }
                } ) ) );
    }
}
