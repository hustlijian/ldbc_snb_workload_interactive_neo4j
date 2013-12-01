package com.ldbc.socialnet.workload;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.ldbc.driver.Operation;

public class LdbcQuery7 extends Operation<List<LdbcQuery7Result>>
{
    private final long personId;
    private final Date minDateTime;
    private final Date maxDateTime;
    private final long durationMillis;
    private final int limit;

    public LdbcQuery7( long personId, Date maxDateTime, long durationMillis, int limit )
    {
        super();
        this.personId = personId;
        this.maxDateTime = maxDateTime;
        Calendar c = Calendar.getInstance();
        c.clear();
        c.setTime( maxDateTime );
        this.minDateTime = new Date( c.getTimeInMillis() - durationMillis );
        this.durationMillis = durationMillis;
        this.limit = limit;
    }

    public long personId()
    {
        return personId;
    }

    public long durationMillis()
    {
        return durationMillis;
    }

    public long minDateTimeAsMilli()
    {
        return minDateTime.getTime();
    }

    public long maxDateTimeAsMilli()
    {
        return maxDateTime.getTime();
    }

    public int limit()
    {
        return limit;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( durationMillis ^ ( durationMillis >>> 32 ) );
        result = prime * result + limit;
        result = prime * result + ( ( maxDateTime == null ) ? 0 : maxDateTime.hashCode() );
        result = prime * result + ( ( minDateTime == null ) ? 0 : minDateTime.hashCode() );
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
        if ( durationMillis != other.durationMillis ) return false;
        if ( limit != other.limit ) return false;
        if ( maxDateTime == null )
        {
            if ( other.maxDateTime != null ) return false;
        }
        else if ( !maxDateTime.equals( other.maxDateTime ) ) return false;
        if ( minDateTime == null )
        {
            if ( other.minDateTime != null ) return false;
        }
        else if ( !minDateTime.equals( other.minDateTime ) ) return false;
        if ( personId != other.personId ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery7 [personId=" + personId + ", minDateTime=" + minDateTime + ", maxDateTime=" + maxDateTime
               + ", durationMillis=" + durationMillis + ", minDateTimeAsMilli=" + minDateTimeAsMilli()
               + ", maxDateTimeAsMilli=" + maxDateTimeAsMilli() + ", limit=" + limit + "]";
    }
}

// public class LdbcQuery7 extends Operation<List<LdbcQuery7Result>>
// {
// private final long personId;
// private final Date endDateTime;
// private final int durationHours;
// private final int limit;
//
// public LdbcQuery7( long personId, Date endDateTime, int durationHours, int
// limit )
// {
// super();
// this.personId = personId;
// this.endDateTime = endDateTime;
// this.durationHours = durationHours;
// this.limit = limit;
// }
//
// public long personId()
// {
// return personId;
// }
//
// public Date endDateTime()
// {
// return endDateTime;
// }
//
// public int durationHours()
// {
// return durationHours;
// }
//
// public long startDateTimeAsMilli()
// {
// Calendar c = Calendar.getInstance();
// c.clear();
// c.setTime( endDateTime );
// c.add( Calendar.HOUR, -durationHours );
// return c.getTimeInMillis();
// }
//
// public long endDateTimeAsMilli()
// {
// return endDateTime.getTime();
// }
//
// public int limit()
// {
// return limit;
// }
//
// @Override
// public int hashCode()
// {
// final int prime = 31;
// int result = 1;
// result = prime * result + durationHours;
// result = prime * result + ( ( endDateTime == null ) ? 0 :
// endDateTime.hashCode() );
// result = prime * result + limit;
// result = prime * result + (int) ( personId ^ ( personId >>> 32 ) );
// return result;
// }
//
// @Override
// public boolean equals( Object obj )
// {
// if ( this == obj ) return true;
// if ( obj == null ) return false;
// if ( getClass() != obj.getClass() ) return false;
// LdbcQuery7 other = (LdbcQuery7) obj;
// if ( durationHours != other.durationHours ) return false;
// if ( endDateTime == null )
// {
// if ( other.endDateTime != null ) return false;
// }
// else if ( !endDateTime.equals( other.endDateTime ) ) return false;
// if ( limit != other.limit ) return false;
// if ( personId != other.personId ) return false;
// return true;
// }
//
// @Override
// public String toString()
// {
// return "LdbcQuery7 [personId=" + personId + ", endDateTime=" + endDateTime +
// ", durationHours=" + durationHours
// + ", startDateTimeAsMilli=" + startDateTimeAsMilli() +
// ", endDateTimeAsMilli=" + endDateTimeAsMilli()
// + ", limit=" + limit + "]";
// }
// }
