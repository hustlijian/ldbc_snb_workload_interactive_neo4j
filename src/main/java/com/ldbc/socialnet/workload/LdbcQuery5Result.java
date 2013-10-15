package com.ldbc.socialnet.workload;

public class LdbcQuery5Result
{
    private final String forum;
    private final long postCount;
    private final long commentCount;

    public LdbcQuery5Result( String forum, long postCount, long commentCount )
    {
        super();
        this.forum = forum;
        this.postCount = postCount;
        this.commentCount = commentCount;
    }

    public String forum()
    {
        return forum;
    }

    public long postCount()
    {
        return postCount;
    }

    public long commentCount()
    {
        return commentCount;
    }

    public long count()
    {
        return postCount + commentCount;
    }
}
