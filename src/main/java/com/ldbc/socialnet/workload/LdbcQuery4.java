package com.ldbc.socialnet.workload;

import java.util.Date;

import com.ldbc.driver.Operation;

// TODO proper result type
public class LdbcQuery4 extends Operation<Object>
{
    private final long personId;
    private final Date startDate;
    private final int durationDays;

    public LdbcQuery4( long personId, Date startDate, int durationDays )
    {
        super();
        this.personId = personId;
        this.startDate = startDate;
        this.durationDays = durationDays;
    }

    public long getPersonId()
    {
        return personId;
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
