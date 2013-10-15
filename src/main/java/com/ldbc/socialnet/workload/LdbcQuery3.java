package com.ldbc.socialnet.workload;

import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery3 extends Operation<List<LdbcQuery3Result>>
{
    private final long personId;
    private final String countryX;
    private final String countryY;
    private final Date endDate;
    private final int durationDays;

    public LdbcQuery3( long personId, String countryX, String countryY, Date endDate, int durationDays )
    {
        super();
        this.personId = personId;
        this.countryX = countryX;
        this.countryY = countryY;
        this.endDate = endDate;
        this.durationDays = durationDays;
    }

    public long personId()
    {
        return personId;
    }

    public String countryX()
    {
        return countryX;
    }

    public String countryY()
    {
        return countryY;
    }

    public Date endDate()
    {
        return endDate;
    }

    public int durationDays()
    {
        return durationDays;
    }
}
