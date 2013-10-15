package com.ldbc.socialnet.workload;

import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery1 extends Operation<List<LdbcQuery1Result>>
{
    private final String firstName;
    private final int limit;

    public LdbcQuery1( String firstName, int limit )
    {
        super();
        this.firstName = firstName;
        this.limit = limit;
    }

    public String firstName()
    {
        return firstName;
    }

    public int limit()
    {
        return limit;
    }
}
