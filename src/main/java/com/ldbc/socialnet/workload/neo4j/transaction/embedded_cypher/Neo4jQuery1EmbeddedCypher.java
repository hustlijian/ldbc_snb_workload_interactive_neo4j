package com.ldbc.socialnet.workload.neo4j.transaction.embedded_cypher;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;

import com.ldbc.socialnet.workload.Domain;
import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery1Result;
import com.ldbc.socialnet.workload.neo4j.transaction.Neo4jQuery1;

public class Neo4jQuery1EmbeddedCypher implements Neo4jQuery1
{
    @Override
    public String description()
    {
        return query();
    }

    @Override
    public Iterator<LdbcQuery1Result> execute( GraphDatabaseService db, ExecutionEngine engine, LdbcQuery1 params )
    {
        return new ResultIterator(
                engine.execute( query(), buildParams( params.firstName(), params.limit() ) ).iterator() );
    }

    private Map<String, Object> buildParams( String firstName, int limit )
    {
        Map<String, Object> queryParams = new HashMap<String, Object>();
        queryParams.put( "person_first_name", firstName );
        queryParams.put( "limit", limit );
        return queryParams;
    }

    private String query()
    {
        return String.format(

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
    }

    public static class ResultIterator implements Iterator<LdbcQuery1Result>
    {
        private final Iterator<Map<String, Object>> inner;

        public ResultIterator( Iterator<Map<String, Object>> inner )
        {
            this.inner = inner;
        }

        @Override
        public boolean hasNext()
        {
            return inner.hasNext();
        }

        @Override
        public LdbcQuery1Result next()
        {
            Map<String, Object> next = inner.next();
            return new LdbcQuery1Result( (String) next.get( "firstName" ), (String) next.get( "lastName" ),
                    (long) next.get( "birthday" ), (long) next.get( "creation" ), (String) next.get( "gender" ),
                    (String[]) next.get( "languages" ), (String) next.get( "browser" ), (String) next.get( "ip" ),
                    (String[]) next.get( "emails" ), (String) next.get( "personCity" ),
                    (Collection<String>) next.get( "unis" ), (Collection<String>) next.get( "companies" ) );
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

    }
}
