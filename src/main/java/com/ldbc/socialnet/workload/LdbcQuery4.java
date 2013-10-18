package com.ldbc.socialnet.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery4 extends Operation<List<LdbcQuery4Result>>
{
    private final long personId;
    private final Date endDate;
    private final int durationDays;

    public LdbcQuery4( long personId, Date endDate, int durationDays )
    {
        super();
        this.personId = personId;
        this.endDate = endDate;
        this.durationDays = durationDays;
    }

    public long personId()
    {
        return personId;
    }

    public Date endDate()
    {
        return endDate;
    }

    public int durationDays()
    {
        return durationDays;
    }

    public long startDateAsMilli()
    {
        Calendar c = Calendar.getInstance();
        c.setTime( endDate );
        c.add( Calendar.DATE, -durationDays );
        return c.getTimeInMillis();
    }

    public long endDateAsMilli()
    {
        return endDate.getTime();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + durationDays;
        result = prime * result + ( ( endDate == null ) ? 0 : endDate.hashCode() );
        result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery4 other = (LdbcQuery4) obj;
        if ( durationDays != other.durationDays ) return false;
        if ( endDate == null )
        {
            if ( other.endDate != null ) return false;
        }
        else if ( !endDate.equals( other.endDate ) ) return false;
        if ( personId != other.personId ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery4 [personId=" + personId + ", endDate=" + endDate + ", durationDays=" + durationDays + "]";
    }
}
