package com.ldbc.socialnet.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery7 extends Operation<List<LdbcQuery7Result>>
{
    private final long personId;
    private final Date endDateTime;
    private final int durationHours;

    public LdbcQuery7( long personId, Date endDateTime, int durationHours )
    {
        super();
        this.personId = personId;
        this.endDateTime = endDateTime;
        this.durationHours = durationHours;
    }

    public long personId()
    {
        return personId;
    }

    public Date endDateTime()
    {
        return endDateTime;
    }

    public int durationHours()
    {
        return durationHours;
    }

    public long startDateTimeAsMilli()
    {
        Calendar c = Calendar.getInstance();
        c.setTime( endDateTime );
        c.add( Calendar.HOUR, -durationHours );
        return c.getTimeInMillis();
    }

    public long endDateTimeAsMilli()
    {
        return endDateTime.getTime();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + durationHours;
        result = prime * result + ( ( endDateTime == null ) ? 0 : endDateTime.hashCode() );
        result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery7 other = (LdbcQuery7) obj;
        if ( durationHours != other.durationHours ) return false;
        if ( endDateTime == null )
        {
            if ( other.endDateTime != null ) return false;
        }
        else if ( !endDateTime.equals( other.endDateTime ) ) return false;
        if ( personId != other.personId ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery7 [personId=" + personId + ", endDateTime=" + endDateTime + ", durationHours=" + durationHours
               + "]";
    }
}
