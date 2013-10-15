package com.ldbc.socialnet.workload;

import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery6 extends Operation<List<LdbcQuery6Result>>
{
    private final long personId;
    private final String tagName;

    public LdbcQuery6( long personId, String tagName )
    {
        super();
        this.personId = personId;
        this.tagName = tagName;
    }

    public long personId()
    {
        return personId;
    }

    public String tagName()
    {
        return tagName;
    }

}
