package com.ldbc.socialnet.workload;

import java.util.Date;

import com.ldbc.driver.Operation;

// TODO proper result type
public class LdbcQuery7 extends Operation<Object>
{
    private final long personId;
    private final Date endDateTime;
    private final int durationHours;

    public LdbcQuery7( long personId, Date endDateTime, int durationHours )
    {
        super();
        this.personId = personId;
        this.endDateTime = endDateTime;
        this.durationHours = durationHours;
    }

    public long personId()
    {
        return personId;
    }

    public Date endDateTime()
    {
        return endDateTime;
    }

    public int durationHours()
    {
        return durationHours;
    }
}
