package com.ldbc.socialnet.workload;

import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery5 extends Operation<List<LdbcQuery5Result>>
{
    private final long personId;
    private final Date joinDate;

    public LdbcQuery5( long personId, Date joinDate )
    {
        super();
        this.personId = personId;
        this.joinDate = joinDate;
    }

    public long personId()
    {
        return personId;
    }

    public Date joinDate()
    {
        return joinDate;
    }
}
