package com.ldbc.socialnet.workload;

import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>>
{
    private final long personId;
    private final Date endDate;
    private final int durationDays;

    public LdbcQuery4( long personId, Date endDate, int durationDays )
    {
        super();
        this.personId = personId;
        this.endDate = endDate;
        this.durationDays = durationDays;
    }

    public long personId()
    {
        return personId;
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
