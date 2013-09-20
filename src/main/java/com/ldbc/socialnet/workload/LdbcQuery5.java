package com.ldbc.socialnet.workload;

import java.util.Date;

import com.ldbc.driver.Operation;

// TODO proper result type
public class LdbcQuery5 extends Operation<Object>
{
    private final long personId;
    private final Date joinDate;

    public LdbcQuery5( long personId, Date joinDate )
    {
        super();
        this.personId = personId;
        this.joinDate = joinDate;
    }

    public long getPersonId()
    {
        return personId;
    }

    public Date getJoinDate()
    {
        return joinDate;
    }
}
