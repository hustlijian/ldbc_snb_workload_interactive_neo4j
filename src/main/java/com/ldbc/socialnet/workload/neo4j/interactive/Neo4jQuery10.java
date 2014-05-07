package com.ldbc.socialnet.workload.neo4j.interactive;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcQuery10Result;

public interface Neo4jQuery10 extends Neo4jQuery<LdbcQuery10, LdbcQuery10Result> {
    /*
    Q10 - Who to connect with?
        Description
            Users that like me who are not my friends by are my friend's friends.
            Find friends of a friend (excluding me) who post a lot about the interests of the user and little about topics that are not in the interests of the user.
            The search is restricted by the candidate’s horoscope sign.
            The result should contain 10 FOFs, where the difference between the total number of their posts about the interests of the specified user and the total number of their posts
            that are not in the interests of the user, is as large as possible.
            Sort the result descending by the difference mentioned in the previous item, and then ascending by FOF URI.
        Parameter
            person
            horoscopeSign (a number between 1 and 12)
            horoscopeSign + 1
        Result (for each result return)
            Person.firstName
            Person.lastName
            Person.id
            count
            Person.gender
            Person-isLocatedIn->Location.name
     */
}
