package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery1Result;
import com.ldbc.socialnet.workload.LdbcQuery4Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery1;

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

        "MATCH (person:" + Domain.Node.Person + ")\n"

        + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.FIRST_NAME + ")\n"

        + "WHERE person." + Domain.Person.FIRST_NAME + "={ person_first_name }\n"

        + "WITH person\n"

        + "ORDER BY person." + Domain.Person.LAST_NAME + "\n"

        + "LIMIT {limit}\n"

        + "MATCH (person)-[:" + Domain.Rel.IS_LOCATED_IN + "]->(personCity:" + Domain.Node.Place + ":"
                + Domain.Place.Type.City + ")\n"

                + "WITH person, personCity\n"

                + "MATCH (uniCity:" + Domain.Place.Type.City + ")<-[:" + Domain.Rel.IS_LOCATED_IN + "]-(uni:"
                + Domain.Organisation.Type.University + ")<-[studyAt:" + Domain.Rel.STUDY_AT + "]-(person)\n"

                + "WITH collect(DISTINCT (uni." + Domain.Organisation.NAME + " + ', ' + uniCity." + Domain.Place.NAME
                + "+ '(' + studyAt." + Domain.StudiesAt.CLASS_YEAR + " + ')')) AS unis,\n"

                + "  person, personCity\n"

                + "MATCH (companyCountry:" + Domain.Node.Place + ":" + Domain.Place.Type.Country + ")<-[:"
                + Domain.Rel.IS_LOCATED_IN + "]-(company:" + Domain.Organisation.Type.Company + ")<-[worksAt:"
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
    }
}
