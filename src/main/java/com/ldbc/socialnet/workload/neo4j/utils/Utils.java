package com.ldbc.socialnet.workload.neo4j.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Utils
{
    public static String[] copyArrayAndAddElement( String[] oldArray, String newElement )
    {
        if ( null == oldArray )
        {
            return new String[] { newElement };
        }
        else
        {
            String[] newArray = new String[oldArray.length + 1];
            System.arraycopy( oldArray, 0, newArray, 0, oldArray.length );
            newArray[newArray.length - 1] = newElement;
            return newArray;
        }
    }

    public static String stackTraceToString( Exception e )
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter( sw );
        e.printStackTrace( pw );
        return sw.toString();
    }
}
