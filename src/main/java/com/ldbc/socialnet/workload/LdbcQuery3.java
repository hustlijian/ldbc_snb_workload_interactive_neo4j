package com.ldbc.socialnet.workload;

import java.util.Date;

import com.ldbc.driver.Operation;

// TODO proper result type
public class LdbcQuery3 extends Operation<Object>
{
    private final long personId;
    private final String countryX;
    private final String countryY;
    private final Date startDate;
    private final int durationDays;

    public LdbcQuery3( long personId, String countryX, String countryY, Date startDate, int durationDays )
    {
        super();
        this.personId = personId;
        this.countryX = countryX;
        this.countryY = countryY;
        this.startDate = startDate;
        this.durationDays = durationDays;
    }

    public long getPersonId()
    {
        return personId;
    }

    public String getCountryX()
    {
        return countryX;
    }

    public String getCountryY()
    {
        return countryY;
    }

    public Date getStartDate()
    {
        return startDate;
    }

    public int getDurationDays()
    {
        return durationDays;
    }
}
