package com.ldbc.socialnet.workload;

public class LdbcQuery7Result
{
    private final String tagName;
    private final long tagCount;

    public LdbcQuery7Result( String tagName, long tagCount )
    {
        super();
        this.tagName = tagName;
        this.tagCount = tagCount;
    }

    public String tagName()
    {
        return tagName;
    }

    public long tagCount()
    {
        return tagCount;
    }
}
