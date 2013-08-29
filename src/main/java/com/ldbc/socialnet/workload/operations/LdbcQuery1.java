package com.ldbc.socialnet.workload.operations;

import com.ldbc.driver.Operation;

// TODO proper result type
public class LdbcQuery1 extends Operation<Object>
{
    private final String firstName;

    public LdbcQuery1( String firstName )
    {
        super();
        this.firstName = firstName;
    }

    public String getFirstName()
    {
        return firstName;
    }
}
