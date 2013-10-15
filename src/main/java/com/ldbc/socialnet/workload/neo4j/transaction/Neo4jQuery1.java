package com.ldbc.socialnet.workload.neo4j.transaction;

import com.ldbc.socialnet.workload.LdbcQuery1;
import com.ldbc.socialnet.workload.LdbcQuery1Result;

public interface Neo4jQuery1 extends Neo4jQuery<LdbcQuery1, LdbcQuery1Result>
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
}
