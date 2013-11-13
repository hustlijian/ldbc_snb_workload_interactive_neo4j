package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery1;

import static com.ldbc.socialnet.workload.Domain.*;

public class Neo4jQuery1EmbeddedCypher implements Neo4jQuery1
{
    @Override
    public String description()
    {
        return query();
    }

    @Override
    public Iterator<LdbcQuery1Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery1 operation )
    {
        return Iterators.transform( engine.execute( query(), buildParams( operation ) ).iterator(),
                new Function<Map<String, Object>, LdbcQuery1Result>()
                {
                    @Override
                    public LdbcQuery1Result apply( Map<String, Object> input )
                    {
                        return new LdbcQuery1Result( (String) input.get( "firstName" ),
                                (String) input.get( "lastName" ), (long) input.get( "birthday" ),
                                (long) input.get( "creation" ), (String) input.get( "gender" ),
                                (String[]) input.get( "languages" ), (String) input.get( "browser" ),
                                (String) input.get( "ip" ), (String[]) input.get( "emails" ),
                                (String) input.get( "personCity" ), (Collection<String>) input.get( "unis" ),
                                (Collection<String>) input.get( "companies" ) );
                    }
                } );
    }

    private Map<String, Object> buildParams( LdbcQuery1 operation )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_first_name", operation.firstName() );
        queryParams.put( "limit", operation.limit() );
        return queryParams;
    }

    private String query()
    {
        return String.format(

        "MATCH (person:" + Nodes.Person + " {" + Person.FIRST_NAME + ":{person_first_name}})\n"

        + "WITH person\n"

        + "ORDER BY person." + Person.LAST_NAME + "\n"

        + "LIMIT {limit}\n"

        + "MATCH (person)-[:" + Rels.IS_LOCATED_IN + "]->(personCity:" + Nodes.Place + ":" + Place.Type.City + ")\n"

        + "MATCH (uniCity:" + Place.Type.City + ")<-[:" + Rels.IS_LOCATED_IN + "]-(uni:" + Organisation.Type.University
                + ")<-[studyAt:" + Rels.STUDY_AT + "]-(person)\n"

                + "WITH collect(DISTINCT (uni." + Organisation.NAME + " + ', ' + uniCity." + Place.NAME
                + "+ '(' + studyAt." + StudiesAt.CLASS_YEAR + " + ')')) AS unis,\n"

                + "  person, personCity\n"

                + "MATCH (companyCountry:" + Nodes.Place + ":" + Place.Type.Country + ")<-[:" + Rels.IS_LOCATED_IN
                + "]-(company:" + Organisation.Type.Company + ")<-[worksAt:" + Rels.WORKS_AT + "]-(person)\n"

                + "WITH collect(DISTINCT (company." + Organisation.NAME + " + ', ' + companyCountry." + Place.NAME
                + " + '('+ worksAt." + WorksAt.WORK_FROM + " + ')')) AS companies,\n"

                + "  unis, person, personCity\n"

                + "RETURN person.%s AS firstName, person.%s AS lastName, person.%s AS birthday,\n"

                + "  person.%s AS creation, person.%s AS gender, person.%s AS languages,\n"

                + "  person.%s AS browser, person.%s AS ip, person.%s AS emails,\n"

                + "  personCity.%s AS personCity, unis, companies",

        Person.FIRST_NAME, Person.LAST_NAME, Person.BIRTHDAY, Person.CREATION_DATE, Person.GENDER, Person.LANGUAGES,
                Person.BROWSER_USED, Person.LOCATION_IP, Person.EMAIL_ADDRESSES, Place.NAME );
    }
}
