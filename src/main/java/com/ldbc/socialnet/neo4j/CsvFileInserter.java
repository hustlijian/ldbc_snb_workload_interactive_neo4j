package com.ldbc.socialnet.neo4j;

import java.io.File;
import java.io.FileNotFoundException;

public class CsvFileInserter
{
    private static final int DEFAULT_BUFFER_SIZE = 10000;

    private final String[][] csvReadBuffer;
    private final File file;
    private final CsvFileReader csvReader;
    private final CsvLineInserter lineInserter;
    private final int bufferSize;

    public CsvFileInserter( File file, CsvLineInserter lineInserter ) throws FileNotFoundException
    {
        this( DEFAULT_BUFFER_SIZE, file, lineInserter );
    }

    private CsvFileInserter( int bufferSize, File file, CsvLineInserter lineInserter ) throws FileNotFoundException
    {
        super();
        this.bufferSize = bufferSize;
        this.csvReadBuffer = new String[bufferSize][];
        this.file = file;
        this.csvReader = new CsvFileReader( file );
        this.lineInserter = lineInserter;
    }

    public File getFile()
    {
        return file;
    }

    private int bufferLines()
    {
        for ( int line = 0; line < bufferSize; line++ )
        {
            if ( csvReader.hasNext() )
            {
                csvReadBuffer[line] = csvReader.next();
            }
            else
            {
                return line + 1;
            }
        }
        return bufferSize;
    }

    public int insertAllBuffered() throws FileNotFoundException
    {
        int count = 0;
        int bufferedLines = bufferLines();
        while ( bufferedLines > 0 )
        {
            for ( int line = 0; line < bufferedLines; line++ )
            {
                lineInserter.insertLine( csvReadBuffer[line] );
                count++;
            }
            bufferedLines = bufferLines();
        }
        return count;
    }

    public int insertAll() throws FileNotFoundException
    {
        int count = 0;
        while ( csvReader.hasNext() )
        {
            lineInserter.insertLine( csvReader.next() );
            count++;
        }
        return count;
    }
}
