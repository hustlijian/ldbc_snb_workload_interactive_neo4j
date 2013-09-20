package com.ldbc.socialnet.workload;

import com.ldbc.driver.Operation;

// TODO proper result type
public class LdbcQuery6 extends Operation<Object>
{
    private final long personId;
    private final String tagName;

    public LdbcQuery6( long personId, String tagName )
    {
        super();
        this.personId = personId;
        this.tagName = tagName;
    }

    public long getPersonId()
    {
        return personId;
    }

    public String getTagName()
    {
        return tagName;
    }

}
