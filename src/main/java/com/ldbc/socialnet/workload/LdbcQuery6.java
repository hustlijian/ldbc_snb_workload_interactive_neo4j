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

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
        result = prime * result + ( ( tagName == null ) ? 0 : tagName.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery6 other = (LdbcQuery6) obj;
        if ( personId != other.personId ) return false;
        if ( tagName == null )
        {
            if ( other.tagName != null ) return false;
        }
        else if ( !tagName.equals( other.tagName ) ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery6 [personId=" + personId + ", tagName=" + tagName + "]";
    }
}
