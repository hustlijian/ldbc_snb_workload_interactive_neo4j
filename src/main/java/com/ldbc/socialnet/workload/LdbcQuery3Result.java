package com.ldbc.socialnet.workload;

public class LdbcQuery3Result
{
    private final String friendName;
    private final long xCount;
    private final long yCount;
    private final long xyCount;

    public LdbcQuery3Result( String friendName, long xCount, long yCount, long xyCount )
    {
        super();
        this.friendName = friendName;
        this.xCount = xCount;
        this.yCount = yCount;
        this.xyCount = xyCount;
    }

    public String friendName()
    {
        return friendName;
    }

    public long xCount()
    {
        return xCount;
    }

    public long yCount()
    {
        return yCount;
    }

    public long xyCount()
    {
        return xyCount;
    }

}
