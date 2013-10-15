package com.ldbc.socialnet.workload;

public class LdbcQuery4Result
{
    private final String tagName;
    private final int tagCount;

    public LdbcQuery4Result( String tagName, int tagCount )
    {
        super();
        this.tagName = tagName;
        this.tagCount = tagCount;
    }

    public String tagName()
    {
        return tagName;
    }

    public int tagCount()
    {
        return tagCount;
    }
}
