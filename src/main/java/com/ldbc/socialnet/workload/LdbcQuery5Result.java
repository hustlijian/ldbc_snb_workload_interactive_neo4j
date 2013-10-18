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

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( commentCount ^ ( commentCount >>> 32 ) );
        result = prime * result + ( ( forum == null ) ? 0 : forum.hashCode() );
        result = prime * result + (int) ( postCount ^ ( postCount >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery5Result other = (LdbcQuery5Result) obj;
        if ( commentCount != other.commentCount ) return false;
        if ( forum == null )
        {
            if ( other.forum != null ) return false;
        }
        else if ( !forum.equals( other.forum ) ) return false;
        if ( postCount != other.postCount ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery5Result [forum=" + forum + ", postCount=" + postCount + ", commentCount=" + commentCount + "]";
    }
}
