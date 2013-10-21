package com.ldbc.socialnet.neo4j.workload;

import java.util.HashMap;
import java.util.Map;

import com.ldbc.socialnet.workload.Domain;

public class TestQueries
{
    public static class PersonTestQuery
    {
        public static final String ID_QUERY_TEMPLATE =

        "MATCH (person:" + Domain.Node.Person + ")\n"

        + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.ID + ")\n"

        + "WHERE person." + Domain.Person.ID + "={id}\n"

        + "RETURN person";

        public static final String FIRST_NAME_QUERY_TEMPLATE =

        "MATCH (person:" + Domain.Node.Person + ")\n"

        + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.FIRST_NAME + ")\n"

        + "WHERE person." + Domain.Person.FIRST_NAME + "={first_name} AND person." + Domain.Person.LAST_NAME
                + "={last_name}\n"

                + "RETURN person";

        public static final String LAST_NAME_QUERY_TEMPLATE =

        "MATCH (person:" + Domain.Node.Person + ")\n"

        + "USING INDEX person:" + Domain.Node.Person + "(" + Domain.Person.LAST_NAME + ")\n"

        + "WHERE person." + Domain.Person.FIRST_NAME + "={first_name} AND person." + Domain.Person.LAST_NAME
                + "={last_name}\n"

                + "RETURN person";

        public static final Map<String, Object> buildParams( long personId, String firstName, String lastName )
        {
            Map<String, Object> queryParams = new HashMap<String, Object>();
            queryParams.put( "id", personId );
            queryParams.put( "first_name", firstName );
            queryParams.put( "last_name", lastName );
            return queryParams;
        }
    }

    public static class PlaceTestQuery
    {
        public static final String CITY_PLACE_NAME_QUERY_TEMPLATE =

        "MATCH (place:" + Domain.Node.Place + ")\n"

        + "USING INDEX place:" + Domain.Node.Place + "(" + Domain.Place.NAME + ")\n"

        + "WHERE place." + Domain.Place.NAME + "={city_name}\n"

        + "RETURN place";

        public static final String COUNTRY_PLACE_NAME_QUERY_TEMPLATE =

        "MATCH (place:" + Domain.Node.Place + ")\n"

        + "USING INDEX place:" + Domain.Node.Place + "(" + Domain.Place.NAME + ")\n"

        + "WHERE place." + Domain.Place.NAME + "={country_name}\n"

        + "RETURN place";

        public static final String CITY_NAME_QUERY_TEMPLATE =

        "MATCH (place:" + Domain.Place.Type.City + ")\n"

        + "USING INDEX place:" + Domain.Place.Type.City + "(" + Domain.Place.NAME + ")\n"

        + "WHERE place." + Domain.Place.NAME + "={city_name}\n"

        + "RETURN place";

        public static final String COUNTRY_NAME_QUERY_TEMPLATE =

        "MATCH (place:" + Domain.Place.Type.Country + ")\n"

        + "USING INDEX place:" + Domain.Place.Type.Country + "(" + Domain.Place.NAME + ")\n"

        + "WHERE place." + Domain.Place.NAME + "={country_name}\n"

        + "RETURN place";

        public static final Map<String, Object> buildParams( String cityName, String countryName )
        {
            Map<String, Object> queryParams = new HashMap<String, Object>();
            queryParams.put( "city_name", cityName );
            queryParams.put( "country_name", countryName );
            return queryParams;
        }

    }
}
